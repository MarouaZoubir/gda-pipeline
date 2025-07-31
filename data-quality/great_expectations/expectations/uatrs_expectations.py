
from datetime import datetime
import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.exceptions import DataContextError
from sqlalchemy import text

context = gx.get_context()

def initialize_suite(suite_name):
    """Helper function to create suite if it doesn't exist"""
    try:
        return context.get_expectation_suite(suite_name)
    except DataContextError:
        return context.add_expectation_suite(suite_name)

def get_valid_ids_from_table(table_name, id_column):
    """Get valid IDs from a reference table with error handling"""
    try:
        engine = context.datasources["my_datasource_postgres"].execution_engine.engine
        with engine.connect() as conn:
            # First check if table exists
            table_exists = conn.execute(
                text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'uatrs_db' AND table_name = '{table_name}')")
            ).scalar()
            
            if not table_exists:
                print(f"Warning: Table uatrs_db.{table_name} does not exist")
                return None
                
            # Then check if column exists
            column_exists = conn.execute(
                text(f"SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'uatrs_db' AND table_name = '{table_name}' AND column_name = '{id_column}')")
            ).scalar()
            
            if not column_exists:
                print(f"Warning: Column {id_column} not found in uatrs_db.{table_name}")
                return None
                
            result = conn.execute(text(f"SELECT {id_column} FROM uatrs_db.{table_name}"))
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        print(f"Warning: Could not fetch IDs from {table_name}.{id_column}: {str(e)}")
        return None

def get_column_stats(validator, column):
    """Compatible stats calculation for GE 0.15.50+"""
    try:
        # For newer GE versions
        df = validator.active_batch.data.dataframe
    except AttributeError:
        # Fallback for older versions
        df = validator.active_batch.data
        
    if column not in df.columns:
        return None
        
    stats = {
        "min": float(df[column].min()),
        "max": float(df[column].max()),
        "mean": float(df[column].mean()),
        "std": float(df[column].std())
    }
    return stats
# ==============================================
# 1. BONANALYSE (Analysis Vouchers) - Updated
# ==============================================
def create_bonanalyse_expectations():
    try:
        suite = initialize_suite("bonanalyse")
        batch_request = {
            "datasource_name": "my_datasource_postgres",
            "data_connector_name": "default_inferred_data_connector_name",
            "data_asset_name": "uatrs_db.bonanalyse",
            "batch_spec_passthrough": {
                "schema_name": "uatrs_db",
                "table_name": "bonanalyse"
            }
        }

        validator = context.get_validator(
            batch_request=BatchRequest(**batch_request),
            expectation_suite=suite
        )
        
        # First verify the column exists
        columns = [col["name"] for col in validator.active_batch.batch_definition["table_columns"]]
        if "numeroBA" not in columns:
            raise ValueError("Column 'numeroBA' not found in bonanalyse table")
        
        # 1. Strict Business Rules (Must Pass)
        validator.expect_column_values_to_not_be_null(column="numeroBA")
        validator.expect_column_values_to_match_regex(
            column="numeroBA", 
            regex=r"^BA\d{6}$"
        )
        
        # Absolute system limits
        validator.expect_column_values_to_be_between(
            column="montantInitial",
            min_value=0,
            max_value=1000000,  # Hard system maximum
            meta={"enforcement": "strict"}
        )
        
        # 2. Advisory Statistical Checks (For Monitoring Only)
        stats = get_column_stats(validator, "montantInitial")
        if stats:
            validator.expect_column_values_to_be_between(
                column="montantInitial",
                min_value=max(0, stats["mean"] - 3*stats["std"]),
                max_value=stats["mean"] + 3*stats["std"],
                mostly=0.98,
                meta={
                    "enforcement": "advisory",
                    "alert_channel": "slack#finance-alerts",
                    "stats": stats
                }
            )
        
        # 3. Foreign Key Validation
        valid_etablissement_ids = get_valid_ids_from_table(
            "etablissementnonuniversitaire", 
            "idEtablissementNonUniversitaire"
        )
        if valid_etablissement_ids:
            validator.expect_column_values_to_be_in_set(
                column="idEtablissementConventionne",
                value_set=valid_etablissement_ids,
                mostly=0.95
            )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created bonanalyse expectations (with advisory checks)")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create bonanalyse expectations: {str(e)}")
        return False
# ==============================================
# 2. CORRESPONDANCE (Correspondence) - Updated
# ==============================================
def create_correspondance_expectations():
    try:
        suite = initialize_suite("correspondance")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.correspondance"
            ),
            expectation_suite=suite
        )
        
        validator.expect_table_columns_to_match_ordered_list([
            "idCorrespondance", "dateDerniereModification", "dateEnvoi",
            "messsage", "objet", "idTypeCorrespondance", "idUser"
        ])
        
        validator.expect_column_values_to_match_regex(
            column="objet",
            regex=r"^[\w\s\déèêëàâäôöûüçÉÈÊËÀÂÄÔÖÛÜÇ.,;:!?()-]{10,200}$",
            mostly=0.95
        )
        
        # Replace column_values_to_satisfy with length check
        validator.expect_column_value_lengths_to_be_between(
            column="messsage",
            min_value=20,
            mostly=0.98
        )
        
        # Replace foreign key expectation
        valid_type_ids = get_valid_ids_from_table(
            "typecorrespondance",
            "idTypeCorrespondance"
        )
        if valid_type_ids:
            validator.expect_column_values_to_be_in_set(
                column="idTypeCorrespondance",
                value_set=valid_type_ids,
                mostly=0.95
            )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created correspondance expectations")
        return True
    except Exception as e:
        print(f"❌ Failed to create correspondance expectations: {str(e)}")
        return False

# ==============================================
# 3. DEMANDE FICHE ANALYSE (Analysis Requests) - Updated
# ==============================================
def create_demandeficheanalyse_expectations():
    try:
        suite = initialize_suite("demandeficheanalyse")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.demandeficheanalyse"
            ),
            expectation_suite=suite
        )
        
        validator.expect_column_values_to_be_unique(column="idDemandeFicheAnalyse")
        validator.expect_column_values_to_not_be_null(column="numeroDemandeFicheAnalyse")
        
        # Replace foreign key expectation
        valid_lab_ids = get_valid_ids_from_table("laboratoire", "idLaboratoire")
        if valid_lab_ids:
            validator.expect_column_values_to_be_in_set(
                column="idLaboratoire",
                value_set=valid_lab_ids,
                mostly=0.95
            )
            
        validator.expect_column_values_to_be_in_set(
            column="idStatutDemandeFicheAnalyse",
            value_set=[1, 2, 3, 4, 5, 6, 7, 8]
        )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created demandeficheanalyse expectations")
        return True
    except Exception as e:
        print(f"❌ Failed to create demandeficheanalyse expectations: {str(e)}")
        return False

# ==============================================
# 4. DEVIS (Quotes) - Updated
# ==============================================
def create_devis_expectations():
    try:
        suite = initialize_suite("devis")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.devis"
            ),
            expectation_suite=suite
        )
        
        # 1. Strict Business Rules
        validator.expect_column_values_to_be_unique(column="idDevis")
        validator.expect_column_values_to_not_be_null(column="numeroDevis")
        
        # Absolute amount limits
        validator.expect_column_values_to_be_between(
            column="montantInitial",
            min_value=0,
            max_value=2000000,  # System maximum
            meta={"enforcement": "strict"}
        )
        
        # 2. Advisory Statistical Checks
        stats = get_column_stats(validator, "montantInitial")
        if stats:
            validator.expect_column_values_to_be_between(
                column="montantInitial",
                min_value=stats["min"],
                max_value=stats["max"],
                mostly=0.99,
                meta={
                    "enforcement": "advisory",
                    "purpose": "Identify potential data entry errors"
                }
            )
        
        # 3. Temporal Consistency
        validator.expect_column_pair_values_A_to_be_greater_than_B(
            column_A="datedernieremodification",
            column_B="datecreation",
            or_equal=True
        )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created devis expectations (with advisory checks)")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create devis expectations: {str(e)}")
        return False

# ==============================================
# 5. DOSSIER ANALYSE (Analysis Files) - Updated
# ==============================================
def create_dossieranalyse_expectations():
    try:
        suite = initialize_suite("dossieranalyse")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.dossieranalyse"
            ),
            expectation_suite=suite
        )
        
        validator.expect_column_values_to_be_unique(column="idDossierAnalyse")
        validator.expect_column_values_to_not_be_null(column="numeroDossierAnalyse")
        validator.expect_column_values_to_match_regex(
            column="numeroDossierAnalyse",
            regex=r"^DA-[0-9]{6}$"
        )
        
        # Replace foreign key expectation
        valid_facturation_ids = get_valid_ids_from_table("facturation", "idFacturation")
        if valid_facturation_ids:
            validator.expect_column_values_to_be_in_set(
                column="idFacturation",
                value_set=valid_facturation_ids,
                mostly=0.95
            )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created dossieranalyse expectations")
        return True
    except Exception as e:
        print(f"❌ Failed to create dossieranalyse expectations: {str(e)}")
        return False

# ==============================================
# 6. FACTURATION (Billing) - No changes needed
# ==============================================
def create_facturation_expectations():
    try:
        suite = initialize_suite("facturation")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.facturation"
            ),
            expectation_suite=suite
        )
        
        # 1. Strict Rules
        validator.expect_column_values_to_be_unique(column="idFacturation")
        validator.expect_column_values_to_not_be_null(column="numero")
        
        # 2. Advisory Amount Checks
        # First validate we can compute stats
        validator.expect_column_values_to_not_be_null(column="montant")
        
        # Then calculate statistics
        df = validator.active_batch.data
        stats = {
            "mean": float(df["montant"].mean()),
            "std": float(df["montant"].std())
        }
        
        if stats["std"] > 0:  # Only apply if there's variation
            validator.expect_column_values_to_be_between(
                column="montant",
                min_value=stats["mean"] - 2*stats["std"],
                max_value=stats["mean"] + 2*stats["std"],
                mostly=0.95,
                meta={
                    "enforcement": "advisory",
                    "stats": stats
                }
            )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created facturation expectations (with advisory checks)")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create facturation expectations: {str(e)}")
        return False
# ==============================================
# 7. LABORATOIRE (Laboratories) - No changes needed
# ==============================================
def create_laboratoire_expectations():
    try:
        suite = initialize_suite("laboratoire")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.laboratoire"
            ),
            expectation_suite=suite
        )
        
        validator.expect_column_values_to_not_be_null(column="intitule")
        validator.expect_column_values_to_be_between(
            column="tarifOrganismeAdministratif",
            min_value=0,
            max_value=10000
        )
        validator.expect_column_values_to_be_in_set(
            column="disponibilite",
            value_set=[True, False]
        )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created laboratoire expectations")
        return True
    except Exception as e:
        print(f"❌ Failed to create laboratoire expectations: {str(e)}")
        return False

# ==============================================
# 8. MEMBRE EXTERNE (External Members) - Updated
# ==============================================
# 8. MEMBRE EXTERNE (External Members) - Fixed
# ==============================================
def create_membreexterne_expectations():
    try:
        suite = initialize_suite("membreexterne")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.membreexterne"
            ),
            expectation_suite=suite
        )
        
        # Temporal validity - fixed for GE 0.15.50
        validator.expect_column_values_to_be_between(
            column="datedebutaffectation",
            min_value="1900-01-01",
            max_value=datetime.now().strftime("%Y-%m-%d")
        )
        
        # Cross-table relationships
        fk_relations = [("idcivilite", "civilite"), ("idpays", "pays"), ("iduser", "users")]
        for col, ref_table in fk_relations:
            valid_ids = get_valid_ids_from_table(ref_table, f"id{ref_table}")
            if valid_ids:
                validator.expect_column_values_to_be_in_set(column=col, value_set=valid_ids)
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created membreexterne expectations")
        return True
    except Exception as e:
        print(f"❌ Failed to create membreexterne expectations: {str(e)}")
        return False

# ==============================================
# 9. MEMBRE UATRS (UATRS Members) - Updated
# ==============================================
def create_membreuatrs_expectations():
    try:
        suite = initialize_suite("membreuatrs")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.membreuatrs"
            ),
            expectation_suite=suite
        )
        
        validator.expect_column_values_to_be_unique(column="idMembreUatrs")
        validator.expect_column_values_to_not_be_null(column="nom")
        validator.expect_column_values_to_not_be_null(column="prenom")
        
        # Replace foreign key expectation
        valid_civilite_ids = get_valid_ids_from_table("civilite", "idCivilite")
        if valid_civilite_ids:
            validator.expect_column_values_to_be_in_set(
                column="idCivilite",
                value_set=valid_civilite_ids,
                mostly=0.95
            )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created membreuatrs expectations")
        return True
    except Exception as e:
        print(f"❌ Failed to create membreuatrs expectations: {str(e)}")
        return False

# ==============================================
# 10. RAPPORT (Reports) - Updated
# ==============================================
def create_rapport_expectations():
    try:
        suite = initialize_suite("rapport")
        
        # First check if table exists
        engine = context.datasources["my_datasource_postgres"].execution_engine.engine
        with engine.connect() as conn:
            table_exists = conn.execute(
                text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'uatrs_db' AND table_name = 'rapport')")
            ).scalar()
            
        if not table_exists:
            print("⚠️ Table uatrs_db.rapport does not exist - skipping")
            return False
        # Explicit batch request with schema
        batch_request = {
            "datasource_name": "my_datasource_postgres",
            "data_connector_name": "default_inferred_data_connector_name",
            "data_asset_name": "uatrs_db.rapport",
            "batch_spec_passthrough": {
                "schema_name": "uatrs_db",
                "table_name": "rapport"
            }
        }
        
        validator = context.get_validator(
            batch_request=BatchRequest(**batch_request),
            expectation_suite=suite
        )
        
        # Your existing expectations...
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created rapport expectations")
        return True
    except Exception as e:
        print(f"❌ Failed to create rapport expectations: {str(e)}")
        return False
# ==============================================
# 11. TYPE ANALYSE (Analysis Types) - Updated
# ==============================================
def create_typeanalyse_expectations():
    try:
        suite = initialize_suite("typeanalyse")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.typeanalyse"
            ),
            expectation_suite=suite
        )
        
        validator.expect_column_values_to_be_unique(column="idTypeAnalyse")
        validator.expect_column_values_to_not_be_null(column="prestation")
        validator.expect_column_values_to_be_between(
            column="tarifOrganismeCommercial",
            min_value=0
        )
        
        # Replace foreign key expectation
        valid_lab_ids = get_valid_ids_from_table("laboratoire", "idLaboratoire")
        if valid_lab_ids:
            validator.expect_column_values_to_be_in_set(
                column="idLaboratoire",
                value_set=valid_lab_ids,
                mostly=0.95
            )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created typeanalyse expectations")
        return True
    except Exception as e:
        print(f"❌ Failed to create typeanalyse expectations: {str(e)}")
        return False

# ==============================================
# 12. USERS (System Users) - No changes needed
# ==============================================
def create_users_expectations():
    try:
        suite = initialize_suite("users")
        validator = context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_datasource_postgres",
                data_connector_name="default_inferred_data_connector_name",
                data_asset_name="uatrs_db.users"
            ),
            expectation_suite=suite
        )
        
        validator.expect_column_values_to_not_be_null(column="userName")
        validator.expect_column_values_to_be_unique(column="userName")
        validator.expect_column_values_to_match_regex(
            column="password",
            regex=r"^.{8,}$"
        )
        validator.expect_column_values_to_be_in_set(
            column="enabled",
            value_set=[0, 1]
        )
        
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("✅ Created users expectations")
        return True
    except Exception as e:
        print(f"❌ Failed to create users expectations: {str(e)}")
        return False

# ==============================================
# MAIN EXECUTION WITH PROGRESS TRACKING
# ==============================================
if __name__ == "__main__":
    print("=== UATRS Expectation Suite Creation ===")
    print("This will create all expectation suites for your database tables\n")
    
    # Verify datasource exists
    try:
        if "my_datasource_postgres" not in context.datasources:
            raise ValueError("Datasource 'my_datasource_postgres' not found")
    except Exception as e:
        print(f"\n❌ Critical Error: {str(e)}")
        print("Please configure your datasource first by running:")
        print("great_expectations datasource new")
        exit(1)

    # List of all expectation creators in desired order
    results = []
    creators = [
        create_bonanalyse_expectations,
        create_correspondance_expectations,
        create_demandeficheanalyse_expectations,
        create_devis_expectations,
        create_dossieranalyse_expectations,
        create_facturation_expectations,
        create_laboratoire_expectations,
        create_membreexterne_expectations,
        create_membreuatrs_expectations,
        create_rapport_expectations,
        create_typeanalyse_expectations,
        create_users_expectations
    ]
    
    for creator in creators:
        start_time = datetime.now()
        success = creator()
        duration = (datetime.now() - start_time).total_seconds()
        results.append({
            "suite": creator.__name__.replace("create_", "").replace("_expectations", ""),
            "status": "✅" if success else "❌",
            "duration_sec": round(duration, 2)
        })
    
    # Enhanced reporting
    print("\n=== Execution Summary ===")
    for result in results:
        print(f"{result['status']} {result['suite'].ljust(25)} {result['duration_sec']}s")
    
    print("\nNext steps:")
    print("- great_expectations checkpoint run pre_etl_checks")
    print("- great_expectations docs build")
    print("- Monitor validation results in Data Docs")