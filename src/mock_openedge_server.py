from http.server import HTTPServer, BaseHTTPRequestHandler
import json

class MockOpenEdgeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if "/rest/enterprise_app/api/inventory" in self.path or "/rest/enterprise_app/inventory" in self.path:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            
            payload = [
                {"product_id": "OE-1001", "product_name": "Industrial Valve A", "quantity_available": 150, "unit_price": 45.50, "status": "ACTIVE"},
                {"product_id": "OE-1002", "product_name": "Hydraulic Seal B", "quantity_available": 12, "unit_price": 120.00, "status": "LOW_STOCK"},
                {"product_id": "OE-1003", "product_name": "Sensor Module C", "quantity_available": 0, "unit_price": 275.00, "status": "DISCONTINUED"}
            ]
            self.wfile.write(json.dumps(payload).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error": "Endpoint not found"}')

def run_server(port=8810):
    server_address = ('localhost', port)
    httpd = HTTPServer(server_address, MockOpenEdgeHandler)
    print(f"🚀 Mock OpenEdge PASOE REST Server running at http://localhost:{port}/rest/enterprise_app/inventory")
    httpd.serve_forever()

if __name__ == "__main__":
    run_server()