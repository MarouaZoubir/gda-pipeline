#!/usr/bin/env python3
"""
create_all_uatrs_checkpoints.py - Crée tous les checkpoints UATRS
"""

import great_expectations as gx
import os
import sys
import yaml

context = gx.get_context()

# Configuration complète pour toutes les tables
CHECKPOINT_CONFIGS = {
    "bonanalyse": {
        "expectation_suite_name": "bonanalyse",
        "table_name": "uatrs_db.bonanalyse",
        "custom_query": None
    },
    "correspondance": {
        "expectation_suite_name": "correspondance",
        "table_name": "uatrs_db.correspondance",
        "custom_query": None
    },
    "demandeficheanalyse": {
        "expectation_suite_name": "demandeficheanalyse",
        "table_name": "uatrs_db.demandeficheanalyse",
        "custom_query": None
    },
    "devis": {
        "expectation_suite_name": "devis",
        "table_name": "uatrs_db.devis",
        "custom_query": None
    },
    "dossieranalyse": {
        "expectation_suite_name": "dossieranalyse",
        "table_name": "uatrs_db.dossieranalyse",
        "custom_query": None
    },
    "facturation": {
        "expectation_suite_name": "facturation",
        "table_name": "uatrs_db.facturation",
        "custom_query": None
    },
    "laboratoire": {
        "expectation_suite_name": "laboratoire",
        "table_name": "uatrs_db.laboratoire",
        "custom_query": None
    },
    "membreexterne": {
        "expectation_suite_name": "membreexterne",
        "table_name": "uatrs_db.membreexterne",
        "custom_query": "SELECT * FROM uatrs_db.membreexterne WHERE isactive IS NOT NULL"
    },
    "membreuatrs": {
        "expectation_suite_name": "membreuatrs",
        "table_name": "uatrs_db.membreuatrs",
        "custom_query": None
    },
    "rapport": {
        "expectation_suite_name": "rapport",
        "table_name": "uatrs_db.rapport",
        "custom_query": None
    },
    "typeanalyse": {
        "expectation_suite_name": "typeanalyse",
        "table_name": "uatrs_db.typeanalyse",
        "custom_query": None
    },
    "users": {
        "expectation_suite_name": "users",
        "table_name": "uatrs_db.users",
        "custom_query": None
    }
}

def create_checkpoint(checkpoint_name, config):
    """Crée un checkpoint et le fichier YAML correspondant"""
    try:
        checkpoint_config = {
            "name": f"{checkpoint_name}_checkpoint",
            "config_version": 1.0,
            "class_name": "SimpleCheckpoint",
            "expectation_suite_name": config["expectation_suite_name"],
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "my_datasource_postgres",
                        "data_connector_name": "default_inferred_data_connector_name",
                        "data_asset_name": config["table_name"],
                        "batch_spec_passthrough": {
                            "schema_name": "uatrs_db",
                            "table_name": config["table_name"].split(".")[1]
                        },
                        **({"runtime_parameters": {"query": config["custom_query"]}} 
                           if config["custom_query"] else {})
                    }
                }
            ],
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"}
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"}
                }
            ]
        }

        checkpoint_path = os.path.join(
            context.root_directory, 
            "checkpoints", 
            f"{checkpoint_name}_checkpoint.yml"
        )
        
        with open(checkpoint_path, "w") as f:
            yaml.dump(checkpoint_config, f, sort_keys=False)
            
        print(f"✅ Checkpoint '{checkpoint_name}_checkpoint' créé avec succès")
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de la création du checkpoint {checkpoint_name}: {str(e)}")
        return False

def main():
    print("=== Création de tous les Checkpoints UATRS ===")
    print("Cette opération peut prendre quelques minutes...\n")
    
    if "my_datasource_postgres" not in context.datasources:
        print("\n❌ Erreur: Datasource 'my_datasource_postgres' introuvable")
        return 1

    results = {}
    for checkpoint_name, config in CHECKPOINT_CONFIGS.items():
        results[checkpoint_name] = create_checkpoint(checkpoint_name, config)

    success_count = sum(results.values())
    total = len(results)
    
    print("\n=== Résumé ===")
    print(f"Checkpoints créés avec succès: {success_count}/{total}")
    
    if success_count < total:
        print("\nCheckpoints non créés:")
        for name, success in results.items():
            if not success:
                print(f"- {name}")
        return 1
    
    print("\n🎉 Tous les checkpoints ont été créés avec succès!")
    print("\nProchaines étapes:")
    print("1. Validez une table spécifique:")
    print("   python run-checkpoints.py --checkpoint NOM_DU_CHECKPOINT")
    print("2. Validez toutes les tables:")
    print("   python run-checkpoints.py")
    return 0

if __name__ == "__main__":
    sys.exit(main())