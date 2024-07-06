# IPZ2
Inzenjerstvo podataka i znanja - projekat 2

## Dataset
[Dataset](https://www.kaggle.com/datasets/anikannal/solar-power-generation-data#) sadrzi podatke o radu 2 solarne elektrane sa 20-ak ploca po elektrani i podacima na svakih 15 minuta. Podaci obuhvataju id ploce, id elektrane, ac i dc power i timestamp kada je izmerena vrednost.

## tcp_server.py
Jednostavan python tcp server koji cita podatke iz oba csv fajla (po 1 za svaku elektranu) i salje na socket podatke. Podaci sa jedne elektrane se salju dok se ne promeni timestamp, kada se prelazi na podatke iz druge elektrane, nakon cega se pravi pauza od 3s. Na pocetak podataka se dodaje drugi id elektrane u vidu indeksa (1, 2...).

## pyspark_app.py
Pyspark app, postavlja master url za spark na "local" i specificira da treba da se koriste 2 jezgra. Podaci prolaze kroz nekoliko dataframe stream-ova: 

- **socket_df** - prima podatke sa tcp servera (localhost:9999)
- **split_df** - splituje podatke sa socket_df po zarezu i imenuje ih 'data'
- **parsed_df** - parsira podatke iz split_df u kolone odgovarajuceg naziva
- **timestamped_wide_df** - dodaje kolone 'timestamp1' i 'timestamp2' koje predstavljaju pokusaje citanja 'timestamp' kolone u 2 razlicita datetime formata
- **timestamped_df** - bira kolonu 'timestamp1' ili 'timestamp2' koja je uspesno parsirana i upisuje u 'timestamp' a potom uklanja kolone 'timestamp1' i 'timestamp2'
- **grouped_df** - pravi window u trajanju od 1h i sa slide-om od 15 min, delayTreshold za podatke se stavlja na 10 min, potom te window-e grupise po 'plant_id' i vrsi sumiranje 'ac_power' i 'dc_power' kolona
Na kraju upisuje podatke u out/ folder u csv formatu.

## coalesce.py
Pyspark app koji cita csv fajlove iz out/ foldera i upisuje ih u final/ folder kao jedan csv fajl.

## big_query_upload.py
Jednostavna python aplikacija koja publishuje podatke iz final.csv fajla u Google BigQuery tabelu. Potrebni kredencijali za bigquery se nalaze u json fajlu u istom folderu.