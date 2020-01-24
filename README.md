# ODCA Search Engine

### API
`GET /schemas` returns first 20 schemas  
`GET /schemas?q={query}` returns schemas which any field matches query  
`GET /schemas?{field1}={query1}&{field2}={query2}&...` returns schemas which given fields matches queries  
`GET /schemas/{hashlink}` returns schema json for given hashlink  
`POST /schemas` store schema given in request body, returns hashlink

`GET /api` redirects to Swagger

### Development

1. Build docker image  
`docker build . -t odca-search-engine`  
1. Create external docker network  
`docker network create odca`  
1. Run  
`docker-compose up`  
It serves:
   1. ODCA Search Engine app on port `9292`
   1. ElasticSearch on port `9200`
   1. Swagger on port `8000`