"""Test CAT data sources and document results."""
import requests
from dotenv import load_dotenv
import os

load_dotenv()


def test_bigquery():
    print("\n[BigQuery] basedosdados")
    try:
        import basedosdados as bd
        
        PROJECT_ID = os.getenv("GCP_PROJECT_ID")
        if not PROJECT_ID:
            print("  GCP_PROJECT_ID not set")
            return
        
        df = bd.read_sql("""
            SELECT table_schema, table_name
            FROM `basedosdados.INFORMATION_SCHEMA.TABLES`
            WHERE LOWER(table_name) LIKE '%cat%'
               OR LOWER(table_name) LIKE '%acidente%'
        """, billing_project_id=PROJECT_ID)
        
        if df.empty:
            print("  No CAT or acidente tables found")
        else:
            print(df.to_string())
    except Exception as e:
        print(f"  Error: {e}")


def test_inss_portal():
    print("\n[INSS] dadosabertos.inss.gov.br")
    try:
        url = "https://dadosabertos.inss.gov.br/api/3/action/package_show"
        r = requests.get(
            url, 
            params={"id": "inss-comunicacao-de-acidente-de-trabalho-cat"}, 
            timeout=15
        )
        print(f"  Status: {r.status_code}")
        if r.ok:
            data = r.json()
            if data.get("success"):
                resources = data.get("result", {}).get("resources", [])
                if resources:
                    print(f"  Found {len(resources)} resources")
                    for res in resources:
                        print(f"    - {res.get('name')} ({res.get('format')})")
                else:
                    print("  No resources")
            else:
                print(f"  API error: {data.get('error', 'Unknown')}")
        else:
            print(f"  Request failed: {r.status_code}")
    except Exception as e:
        print(f"  Error: {e}")


if __name__ == "__main__":
    print("=" * 70)
    print("CAT SOURCE EXPLORATION")
    print("=" * 70)
    
    test_bigquery()
    test_inss_portal()
    
    print("\n" + "=" * 70)
    print("EXPLORATION COMPLETE")
    print("=" * 70)
