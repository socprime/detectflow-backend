from typing import Any

import orjson
import yaml
from schema_parser.manager import ParserManager as SchemaParserManager

from apps.core.schemas import ParserRunResultItem
from apps.modules.kafka.parsers import KafkaParsersSyncService


class ParserManager:
    parser_manager = SchemaParserManager()
    kafka_manager = KafkaParsersSyncService()

    async def test_parser_config(
        self, topics: list[str], parser_config: dict, field_mapping: str | None = None
    ) -> list[ParserRunResultItem]:
        try:
            field_mapping = self._get_field_mapping_from_str(field_mapping)
        except ValueError:
            field_mapping = {}

        results: list[ParserRunResultItem] = []
        for topic in topics:
            source_data = await self.kafka_manager.get_events(topic=topic, limit=20)
            for event in source_data:
                try:
                    parsed_data = self.parser_manager.configured_parser(
                        event=orjson.loads(event), parser_config=parser_config, flatten=True
                    )
                    success = True
                    error_message = None
                except Exception as e:
                    parsed_data = None
                    success = False
                    error_message = str(e)

                if success:
                    self._apply_field_mapping(parsed_data, field_mapping)

                results.append(
                    ParserRunResultItem(
                        source_topic=topic,
                        source_data=event,
                        parsed_data=parsed_data,
                        success=success,
                        error_message=error_message,
                    )
                )
        return results

    async def run_test_parser_query(
        self, topics: list[str], parser_query: str, field_mapping: str | None = None
    ) -> list[ParserRunResultItem]:
        parser_config = self.parser_manager.query_parser(query=parser_query)
        return await self.test_parser_config(topics=topics, parser_config=parser_config, field_mapping=field_mapping)

    async def create_parser_config(self, parser_query: str) -> dict[str, Any]:
        return self.parser_manager.query_parser(query=parser_query)

    @staticmethod
    def _apply_field_mapping(data: dict[str, Any], field_mapping: dict[str, list[str]]) -> None:
        for sigma_field, event_fields in field_mapping.items():
            for event_field in event_fields:
                if event_field in data:
                    data[sigma_field] = data.pop(event_field)

    @staticmethod
    def _get_field_mapping_from_str(field_mapping_str: str | None) -> dict[str, list[str]]:
        """Parse yaml string to dict representing the field mapping.

        Args:
            field_mapping_str: YAML string representing the field mapping

        Example:

        field_mapping:
          ```yaml
          sigma_field: event_field
          sigma_field2:
            - event_field2_1
            - event_field2_2
          ```
        result:
        ```json
        {
          "sigma_field": [
            "event_field"
          ],
          "sigma_field2": [
            "event_field2_1",
            "event_field2_2"
          ]
        }
        ```

        Raises:
            ValueError: if field_mapping_str is not correctly formatted.
        """
        if not field_mapping_str:
            return {}

        try:
            d = yaml.safe_load(field_mapping_str)
        except yaml.YAMLError:
            raise ValueError("field mapping must be a valid YAML string")

        if not isinstance(d, dict):
            raise ValueError("field mapping must be a dictionary")

        field_mapping: dict[str, list[str]] = {}
        for k, v in d.items():
            if isinstance(v, str):
                if not v:
                    continue
                field_mapping[k] = [v]
            elif isinstance(v, list):
                if not v:
                    continue
                field_mapping[k] = []
                for item in v:
                    if not isinstance(item, str):
                        raise ValueError(f"invalid field mapping for key {k}")
                    field_mapping[k].append(item)
            elif v is None:
                continue
            else:
                raise ValueError(f"invalid field mapping for key {k}")

        return field_mapping


parser_manager = ParserManager()
