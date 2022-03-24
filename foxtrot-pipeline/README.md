# Foxtrot Pipelines
Pipelines add a way to mutate/augment documents before storing them.

Pipelines are made up of a set of processors, the processors are run before indexing the documents in QueryStore.

Tables can be augmented with default pipeline, this ensures all documents ingested in the table will go through the pipeline processors.

Example Processor

```json
{
  "name": "AUGMENT-REGION-GRIDS",
  "processors": [
    {
      "type": "REGION_MATCHER",
      "matchField": "$.user.location",
      "targetFieldRoot": "$.user.target.state",
      "geoJsonSource": "res://states-geojson.json",
      "targetWriteMode": "CREATE_ONLY"
    },
    {
      "type": "LOWER",
      "matchField": "$.tenantName"
    },
    {
      "type": "S2_GRID",
      "matchField": "$.location",
      "targetFieldRoot": "$.target.s2cells",
      "s2Levels": [
        1,5,10,12
      ],
      "targetWriteMode": "OVERWRITE"
    }
  ],
  "ignoreErrors": true,
  "executionMode": "SERIAL"
}
```