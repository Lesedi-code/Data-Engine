import http.server
import json
import os
import pandas as pd
from urllib.parse import urlparse, parse_qs

PORT = 8080
RAW_DIR = "Data/raw/structured_dataset"
SUPPORTED_EXTENSIONS = ('.csv', '.json', '.parquet', '.xlsx')

class OpenEdgeMockHandler(http.server.BaseHTTPRequestHandler):
    dataset_cache = None
    active_dataset_name = "default"

    @classmethod
    def load_data(cls):
        if cls.dataset_cache is None:
            cls.dataset_cache = []
            if os.path.exists(RAW_DIR):
                files = [f for f in os.listdir(RAW_DIR) if f.lower().endswith(SUPPORTED_EXTENSIONS) and not f.startswith('.')]
                if files:
                    dataset_file = files[0]
                    # Keep the exact filename as it is stored in the folder
                    cls.active_dataset_name = dataset_file
                    
                    dataset_path = os.path.join(RAW_DIR, dataset_file)
                    print(f"[*] Loading dataset using exact filename: {dataset_file}")
                    
                    try:
                        if dataset_file.endswith('.json'):
                            with open(dataset_path, 'r', encoding='utf-8') as f:
                                raw_data = json.load(f)
                            cls.dataset_cache = raw_data["data"] if isinstance(raw_data, dict) and "data" in raw_data else raw_data
                        elif dataset_file.endswith('.csv'):
                            df = pd.read_csv(dataset_path, low_memory=False)
                            cls.dataset_cache = df.to_dict(orient='records')
                        elif dataset_file.endswith('.parquet'):
                            df = pd.read_parquet(dataset_path)
                            cls.dataset_cache = df.to_dict(orient='records')
                        elif dataset_file.endswith(('.xlsx', '.xls')):
                            df = pd.read_excel(dataset_path)
                            cls.dataset_cache = df.to_dict(orient='records')
                            
                        print(f"🚀 Cached {len(cls.dataset_cache):,} records for: '{cls.active_dataset_name}'")
                    except Exception as e:
                        print(f"❌ Error loading file {dataset_file}: {e}")
            
            if not cls.dataset_cache:
                cls.active_dataset_name = "fallback.csv"
                cls.dataset_cache = [{"id": 1, "name": "Fallback Item", "price": 10.0}]
                
        return cls.dataset_cache

    def do_GET(self):
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)

        data = self.load_data()

        limit = int(query_params.get("limit", [len(data)])[0])
        offset = int(query_params.get("offset", [0])[0])
        paginated_data = data[offset:offset + limit]

        response_payload = {
            "status": "success",
            "dataset": self.active_dataset_name,
            "total_records": len(data),
            "returned_records": len(paginated_data),
            "offset": offset,
            "data": paginated_data
        }

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-OpenEdge-Server", "PASOE/12.2.0")
        self.end_headers()
        self.wfile.write(json.dumps(response_payload, default=str).encode("utf-8"))

    def log_message(self, format, *args):
        print(f"[PASOE Mock Gateway] {args[0]}")

if __name__ == "__main__":
    OpenEdgeMockHandler.load_data()
    server = ('', PORT)
    httpd = http.server.HTTPServer(server, OpenEdgeMockHandler)
    print(f"🚀 Mock Progress OpenEdge PASOE REST API running on http://localhost:{PORT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[*] Shutting down mock server gracefully.")
        httpd.server_close()