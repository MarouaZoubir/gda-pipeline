#!/usr/bin/env python3
"""
Script d'exécution des checkpoints - Version corrigée
"""

import great_expectations as gx
import argparse
import sys
from datetime import datetime
import json

context = gx.get_context()

def run_checkpoint(checkpoint_name, custom_query=None):
    """Exécute un checkpoint spécifique"""
    try:
        # Ne pas spécifier à nouveau le batch_request ici
        result = context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            run_name=f"{checkpoint_name}_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            **({"batch_request": {"runtime_parameters": {"query": custom_query}}} 
               if custom_query else {})
        )
        
        context.build_data_docs()
        return result
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution de {checkpoint_name}: {str(e)}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Exécuteur de validations de données")
    parser.add_argument(
        "--checkpoint", 
        "-c",
        help="Nom du checkpoint à exécuter (ex: bonanalyse_checkpoint)"
    )
    parser.add_argument(
        "--query",
        "-q",
        help="Requête SQL personnalisée pour filtrer les données"
    )
    args = parser.parse_args()

    if args.checkpoint:
        results = {args.checkpoint: run_checkpoint(args.checkpoint, args.query)}
    else:
        results = {
            cp: run_checkpoint(cp) 
            for cp in context.list_checkpoints()
        }
    
    # Génération du rapport
    success_count = sum(1 for r in results.values() if r and r["success"])
    total = len(results)
    
    print("\n=== Résumé ===")
    for name, result in results.items():
        status = "✅ SUCCÈS" if result and result["success"] else "❌ ÉCHEC"
        print(f"{name.ljust(30)} {status}")
    
    print(f"\nTotal: {success_count}/{total} validations réussies")
    print("\nRapport complet disponible dans:")
    print("great_expectations/uncommitted/data_docs/local_site/index.html")
    
    sys.exit(0 if success_count == total else 1)

if __name__ == "__main__":
    main()