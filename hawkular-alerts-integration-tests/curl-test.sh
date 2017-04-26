#!/usr/bin/env bash
curl -v -H "Hawkular-Tenant: test" -X DELETE http://localhost:8080/hawkular/alerts/triggers/xyz
curl -v -H "Hawkular-Tenant: test" -X POST http://localhost:8080/hawkular/alerts/triggers -d "{\"id\": \"xyz\",\"name\": \"XYZ\",\"description\": \"XYZ for testing1\"}"
curl -v -H "Hawkular-Tenant: test" -X POST http://localhost:8080/hawkular/alerts/triggers -d "{\"id\": \"xyz\",\"name\": \"XYZ\",\"description\": \"XYZ for testing2\"}"
curl -v -H "Hawkular-Tenant: test" -X DELETE http://localhost:8080/hawkular/alerts/triggers/xyz
curl -v -H "Hawkular-Tenant: test" -X POST http://localhost:8080/hawkular/alerts/triggers -d "{\"id\": \"xyz\",\"name\": \"XYZ\",\"description\": \"XYZ for testing3\"}"
