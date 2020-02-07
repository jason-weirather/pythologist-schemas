#from jsonschema import validate


panel_schema = {
    "$schema": "https://json-schema.org/schema#",
    "type": "object",
    "properties": {
        "Specification": {
            "type": "object",
            "properties": {
                "Stains":{
                    "type": "object",
                    "additionalProperties": {
                        "type": "string",
                        "description": "Stain names are all assigned string abbreviations."
                    }
                },
                "Rules":{
                    "type":"object",
                    "properties":{
                        "Phenotypes":{
                            "type":"array",
                             "description": "Names used to phenotype cells."
                        },
                        "Thresholds":{
                            "type":"array",
                             "description": "Stain abbreviations that have positive/negative scoring threshold data."
                        },
                        "Regions":{
                            "type":"array"
                        },
                    }
                }
            },
            "required":['Stains','Rules']
                    
        },
        "Meta": {
            "type": "object",
            "properties":{
                "Name":{
                    "type":"string",
                    "description":"The panel name."
                },
                "Description":{
                    "type":"string",
                    "description":"A description of the panel."
                },
                "Version":{
                    "type":"string",
                    "description":"A version of the panel."
                },
                "Platform":{
                    "type":"string",
                    "description":"The platform this panel was built for."
                }
            }
        },
    },
    "required": ["Specification","Meta"]
}