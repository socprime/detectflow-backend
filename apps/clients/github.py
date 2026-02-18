import asyncio
import re
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID

import yaml
from pydantic import BaseModel


class GitHubClientError(Exception):
    """Exception raised for errors in the GitHub client."""

    pass


class GitHubClientNotFoundError(Exception):
    """Exception raised for not found errors in the GitHub client."""

    pass


class GitHubFileContent(BaseModel):
    """Content of a file from GitHub."""

    file_path: str
    file_name: str
    content: str


class GithubRule(BaseModel):
    """Represents a rule parsed from GitHub repository."""

    id: str
    name: str
    body: str


class GitHubClient:
    """Client for GitHub that clones repositories and walks through files locally."""

    _executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="github-file-io")

    def __init__(self, temp_dir: str | None = None, keep_clone: bool = False):
        """Initialize GitHub clone client.

        Args:
            temp_dir: Optional temporary directory for clones. If None, uses system temp.
            keep_clone: If True, keeps the cloned repository after use (for debugging).
        """
        self.temp_dir = temp_dir
        self.keep_clone = keep_clone
        self._clone_path: Path | None = None

    def _parse_github_url(self, url: str) -> tuple[str, str, str]:
        """Parse GitHub URL to extract owner, repo, and path.

        Supports URLs like:
        - https://github.com/owner/repo
        - https://github.com/owner/repo/tree/branch/path/to/dir
        - https://github.com/owner/repo/blob/branch/path/to/file

        Args:
            url: GitHub repository URL

        Returns:
            Tuple of (owner, repo, path) where path is relative to repo root

        Raises:
            GitHubClientError: If URL format is invalid
        """
        parsed = urlparse(url)
        if parsed.netloc not in ("github.com", "www.github.com"):
            raise GitHubClientError(f"Invalid GitHub URL: {url}")

        path_parts = [p for p in parsed.path.split("/") if p]

        if len(path_parts) < 2:
            raise GitHubClientError(f"Invalid GitHub URL format: {url}")

        owner = path_parts[0]
        repo = path_parts[1]

        # Remove 'tree', 'blob', and branch name from path if present
        path = ""
        if len(path_parts) > 2:
            # Skip 'tree' or 'blob' and branch name
            if path_parts[2] in ("tree", "blob") and len(path_parts) > 3:
                # path_parts[3] is the branch name, rest is the path
                path = "/".join(path_parts[4:]) if len(path_parts) > 4 else ""
            else:
                # Direct path without tree/blob
                path = "/".join(path_parts[2:])

        return owner, repo, path

    async def _clone_repository(self, owner: str, repo: str, branch: str = "main") -> Path:
        """Clone GitHub repository to a temporary directory.

        Args:
            owner: Repository owner
            repo: Repository name
            branch: Branch name to clone

        Returns:
            Path to the cloned repository

        Raises:
            GitHubClientError: If clone fails
        """
        repo_url = f"https://github.com/{owner}/{repo}.git"
        clone_dir = Path(self.temp_dir) if self.temp_dir else Path(tempfile.gettempdir())
        clone_path = clone_dir / f"{owner}_{repo}_{branch}"

        # Remove existing clone if it exists (unless keep_clone is True)
        # Run in executor to avoid blocking
        path_exists = await asyncio.get_running_loop().run_in_executor(self._executor, lambda: clone_path.exists())
        if path_exists and not self.keep_clone:
            await asyncio.get_running_loop().run_in_executor(
                self._executor, lambda: shutil.rmtree(clone_path, ignore_errors=True)
            )

        # Clone if directory doesn't exist (check in executor to avoid blocking)
        clone_path_exists = await asyncio.get_running_loop().run_in_executor(
            self._executor, lambda: clone_path.exists()
        )
        if not clone_path_exists:
            try:
                # Run git clone with branch
                process = await asyncio.create_subprocess_exec(
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    "--branch",
                    branch,
                    repo_url,
                    str(clone_path),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                stdout, stderr = await process.communicate()

                if process.returncode != 0:
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    # Try main branch if master fails (or vice versa)
                    if branch in ("main", "master"):
                        fallback_branch = "master" if branch == "main" else "main"
                        process = await asyncio.create_subprocess_exec(
                            "git",
                            "clone",
                            "--depth",
                            "1",
                            "--branch",
                            fallback_branch,
                            repo_url,
                            str(clone_path),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                        stdout, stderr = await process.communicate()
                        if process.returncode != 0:
                            raise GitHubClientError(f"Failed to clone repository: {error_msg}")
                    else:
                        raise GitHubClientError(f"Failed to clone repository: {error_msg}")
            except FileNotFoundError:
                raise GitHubClientError("Git is not installed or not in PATH")
            except Exception as e:
                raise GitHubClientError(f"Failed to clone repository: {str(e)}")

        self._clone_path = clone_path
        return clone_path

    def _walk_files(self, root_path: Path, relative_path: Path = Path("")) -> list[Path]:
        """Recursively walk through files in a directory.

        Args:
            root_path: Root directory to walk
            relative_path: Relative path from root (for filtering)

        Returns:
            List of file paths relative to root
        """
        files = []
        current_path = root_path / relative_path if relative_path else root_path

        if not current_path.exists():
            return files

        if current_path.is_file():
            return [relative_path] if relative_path else []

        for item in current_path.iterdir():
            # Skip .git directory
            if item.name == ".git":
                continue

            item_relative = relative_path / item.name if relative_path else Path(item.name)

            if item.is_file():
                files.append(item_relative)
            elif item.is_dir():
                files.extend(self._walk_files(root_path, item_relative))

        return files

    def _matches_pattern(
        self, file_path: str, include_patterns: list[str] | None, exclude_patterns: list[str] | None
    ) -> bool:
        """Check if file path matches include/exclude patterns.

        Args:
            file_path: File path to check
            include_patterns: List of patterns to include (e.g., ['.yml', '.yaml'])
            exclude_patterns: List of patterns to exclude (e.g., ['deprecated'])

        Returns:
            True if file should be included, False otherwise
        """
        # Check exclude patterns first
        if exclude_patterns:
            for pattern in exclude_patterns:
                # Support both substring matching and regex patterns
                if pattern.startswith("^") and pattern.endswith("$"):
                    # Regex pattern
                    if re.search(pattern, file_path):
                        return False
                else:
                    # Simple substring/glob pattern
                    if pattern in file_path or re.match(pattern.replace("*", ".*"), file_path):
                        return False

        # Check include patterns
        if include_patterns:
            for pattern in include_patterns:
                # Support both extension matching and regex patterns
                if pattern.startswith("^") and pattern.endswith("$"):
                    # Regex pattern
                    if re.search(pattern, file_path):
                        return True
                elif pattern.startswith("."):
                    # Extension pattern (e.g., .yml, .yaml)
                    if file_path.endswith(pattern):
                        return True
                else:
                    # Glob or substring pattern
                    if pattern in file_path or re.match(pattern.replace("*", ".*"), file_path):
                        return True
            # If include patterns are specified but none match, exclude
            return False

        # If no include patterns, include all (unless excluded)
        return True

    async def _get_files_content(
        self,
        github_url: str,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        branch: str | None = None,
    ) -> list[GitHubFileContent]:
        """Get content of files from GitHub repository by cloning and walking locally.

        Args:
            github_url: GitHub repository URL (can be root or subdirectory)
            include_patterns: List of patterns to include files (e.g., ['.yml', '.yaml'])
                             Supports extensions (e.g., '.yml'), glob patterns, or regex (^...$)
            exclude_patterns: List of patterns to exclude files (e.g., ['deprecated'])
                             Supports substring matching, glob patterns, or regex (^...$)
            branch: Branch name (default: tries to detect from URL, falls back to 'main')

        Returns:
            List of GitHubFileContent objects with file_path, file_name, and content

        Raises:
            GitHubClientError: If URL is invalid or clone fails
            GitHubClientNotFoundError: If repository or path is not found

        Example:
            >>> client = GitHubCloneClient()
            >>> files = await client.get_files_content(
            ...     "https://github.com/owner/repo",
            ...     include_patterns=[".yml", ".yaml"],
            ...     exclude_patterns=["deprecated"],
            ...     branch="main"
            ... )
        """
        owner, repo, path = self._parse_github_url(github_url)

        # Try to extract branch from URL if not provided
        if not branch:
            parsed = urlparse(github_url)
            path_parts = [p for p in parsed.path.split("/") if p]
            if len(path_parts) > 2 and path_parts[2] in ("tree", "blob") and len(path_parts) > 3:
                branch = path_parts[3]
            else:
                branch = "main"

        # Clone repository
        clone_path = await self._clone_repository(owner, repo, branch)

        # Determine the base path within the clone
        walk_base = clone_path
        if path:
            walk_base = clone_path / path
            # Check if path exists in executor to avoid blocking
            path_exists = await asyncio.get_running_loop().run_in_executor(self._executor, lambda: walk_base.exists())
            if not path_exists:
                raise GitHubClientNotFoundError(f"Path not found in repository: {path}")

        # Walk through files from the specified base path (run in executor to avoid blocking)
        all_files = await asyncio.get_running_loop().run_in_executor(self._executor, self._walk_files, walk_base)

        # Filter files by patterns and read content (run file reads in executor)
        filtered_files = []
        for file_relative in all_files:
            file_path_str = str(file_relative).replace("\\", "/")  # Normalize path separators

            if self._matches_pattern(file_path_str, include_patterns, exclude_patterns):
                full_file_path = walk_base / file_relative
                try:
                    # Read file content in executor to avoid blocking event loop
                    # Capture full_file_path in lambda with default argument to avoid closure issue
                    content = await asyncio.get_running_loop().run_in_executor(
                        self._executor,
                        lambda fp=full_file_path: fp.read_text(encoding="utf-8"),
                    )
                    file_name = file_relative.name

                    filtered_files.append(
                        GitHubFileContent(
                            file_path=file_path_str,
                            file_name=file_name,
                            content=content,
                        )
                    )
                except UnicodeDecodeError:
                    # Skip binary files
                    continue
                except Exception:
                    # Skip files that can't be read
                    continue

        # Clean up clone if not keeping it (run in executor to avoid blocking)
        if not self.keep_clone and self._clone_path:
            clone_path_to_remove = self._clone_path
            self._clone_path = None
            # Check if exists and remove in executor
            path_exists = await asyncio.get_running_loop().run_in_executor(
                self._executor, lambda: clone_path_to_remove.exists()
            )
            if path_exists:
                await asyncio.get_running_loop().run_in_executor(
                    self._executor, lambda: shutil.rmtree(clone_path_to_remove, ignore_errors=True)
                )

        return filtered_files

    def __del__(self):
        """Cleanup cloned repository on deletion if not keeping it."""
        # Note: This is a fallback cleanup. The async cleanup in _get_files_content is preferred.
        if not self.keep_clone and self._clone_path:
            try:
                if self._clone_path.exists():
                    shutil.rmtree(self._clone_path, ignore_errors=True)
            except Exception:
                pass

    @staticmethod
    def _is_valid_uuid(uuid: str) -> bool:
        """Check if a string is a valid UUID."""
        try:
            UUID(uuid)
            return True
        except (ValueError, TypeError):
            return False

    @classmethod
    def _get_github_rules(cls, files: list[GitHubFileContent]) -> list[GithubRule]:
        """Parse GitHub files and extract valid Sigma rules.

        Args:
            files: List of GitHubFileContent objects to parse

        Returns:
            List of GithubRule objects that are valid Sigma rules
        """
        rules = []
        for file in files:
            try:
                d = yaml.safe_load(file.content)
            except yaml.YAMLError:
                continue
            if not isinstance(d, dict):
                continue
            if not d.get("title"):
                continue
            if not d.get("detection"):
                continue
            rule_id = d.get("id")
            if not cls._is_valid_uuid(rule_id):
                continue
            rules.append(GithubRule(body=file.content, name=d["title"], id=rule_id))
        return rules

    async def get_sigma_rules(
        self,
        github_url: str,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        branch: str | None = None,
    ) -> list[GithubRule]:
        """Get Sigma rules from a GitHub repository.

        Args:
            github_url: GitHub repository URL
            include_patterns: List of patterns to include files (e.g., ['.yml', '.yaml'])
            exclude_patterns: List of patterns to exclude files (e.g., ['deprecated'])
            branch: Branch name to clone (default: tries to detect from URL, falls back to 'main')

        Returns:
            List of GithubRule objects parsed from valid Sigma rule files

        Raises:
            GitHubClientError: If URL is invalid or clone fails
            GitHubClientNotFoundError: If repository or path is not found
        """
        files = await self._get_files_content(
            github_url=github_url,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            branch=branch,
        )
        return self._get_github_rules(files)
