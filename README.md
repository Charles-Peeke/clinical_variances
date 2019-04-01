# Clinical Variances
This program analyzes the variances in keywords, sponsors, and query speeds for a local database. Instructions on downloading the database for local use are [here](https://aact.ctti-clinicaltrials.org/snapshots).

## Production
* Install [PostgreSQL](https://www.postgresql.org/download/) Database Platform
* Download AACT Database, create PostgreSQL database and populate with AACT Database contents (instructions for all steps found [here](https://aact.ctti-clinicaltrials.org/snapshots))


## Speed

Single queries

|         		| Studies      		| Sponsors  	| Keywords  | Studies R.J. Sponsors | Studies R.J. Keywords |
|----------	|--------------	|-----------|-----------|-----------------------|-----------------------|
| Pandas   	|             			|           		|           | 3.309 sec             | 4.583 sec             |
| AACT     	| 30.60702 sec 	| 8.007 sec 	| 9.012 sec | 92.080 sec            | 146.422 sec           |
| Local DB 	| 9.42377 sec  	| 1.408 sec 	| 1.729 sec | 0.389 sec             | 0.816 sec             |