# harvest-sudoc
Harvesting the sudoc

## How to use it ?

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


## API

| endpoint | method |   args   | description |
| -------- | ------ | -------- | ----------- |
| harvest  | POST   | id_refs [str, list]<br>force_download [bool]  | This endpoint will download in ObjectStorage all the sudoc notices for the given id_refs given.<br>If `force_download` is set to `True`, the notice will be downloaded even if already in DB. |
