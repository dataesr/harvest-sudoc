# harvest-sudoc
Harvesting the sudoc

## How to use it

1. Clone the repo

`git clone https://github.com/dataesr/harvest-sudoc.git`

2. Enter the repo

`cd harvest-sudoc`

3. Build the docker image

`make docker-build`

4. Start docker image

`make start`

5. Start the harvesting

`curl -X POST -H 'Content-Type: application/json' -d '{"id_refs": ["02825354X", "242241344", "183975154", "059389451"]}' 'http://localhost:5004/harvest'`


