# Udacity DAND Open Street Maps Data Wrangling - Orlando



## Map Area
--------------


## Wrangling Challanges
--------------------------

## Overview and Data Stats
--------------------------

### File Size
orlando_florida.osm ....... 128M

### Collection Stats
```python
{'num_elem_school': 41,
 'num_high_school': 11,
 'num_publix': 39,
 'total_db_size': 23715840,
 'unique_nodes': 569564,
 'unique_users': 679,
 'unique_ways': 85145}

 def dbStats():
	stats = {
				'total_db_size': db.command('collStats', DBNAME)['storageSize'],
				'unique_users' : len(cur.distinct('userid')),
				'unique_nodes' : cur.count( {'type':'node'} ),
				'unique_ways' : cur.count( {'type':'way'} ),
				'num_publix' : cur.count({'name':re.compile('.*ublix.*')}),
				'num_high_school' : cur.count({'name': re.compile('.*High School.*')}),
				'num_elem_school' : cur.count({'name': re.compile('.*Elementary School.*')}),
				#To do, give top 10 contributors


	}
	
	pprint.pprint( stats)
```

### Top Contributors to Map:
1. NE2 made 284395 contributions
2. 3yoda made 41069 contributions
3. crystalwalrein made 34310 contributions
4. epcotfan made 22993 contributions
5. Brian@Brea made 17348 contributions
6. RobChafer made 16225 contributions
7. dale_p made 15139 contributions
8. KindredCoda made 15024 contributions
9. Adam Martin made 14984 contributions
10. grouper made 13749 contributions


''' python
def topContributors():
	pipeline = [{'$group': {'_id':'$username', 'count': {'$sum':1}}}, \
				{'$sort': {'count':-1}}, \
				{'$project':{ 'userid': 1, 'username':1, 'count':1}}, \
				{'$limit': 10} ]

	print 'Top Contributors to Map:'					
	for i, user in enumerate(cur.aggregate(pipeline), start=1):
		print str(i)+'. '+user['_id']+' made '+str(user['count'])+' contributions'
''''



## Data Overview

## Additional Ideas
### GPS Location Convergence
A lot of the nodes seem to be revisions to the GPS coordinates. A sum of squared computation can reduce the distance down to a scalar metric that can be used to converge the GPS coordinates by rejecting GPS coordinates that differ too drastically from the existing points. 

#### Benefits: 
* This would reduce the number of updates that aren’t useful
* Locations would converge overtime to the average of reported locations making the location more accurate

#### Drawbacks:
* It would much harder to change dramatically different GPS information.  
* It preferences the first people to enter GPS locations and would need to remove outliers to improve the metric. 

## Tag Values Normalized
The tag values have a lot of differing values. My profile method shows 862 different tag value possibilities. The tag element can be more standardized which makes it easier to process location information

#### Benefits
* A more normalized structure makes the data far easier to process
* Anomalies can be found more easily
* Accuracy auditing can be automated by cross referencing the locations against other services.

#### Cons
* The enforced structures could introduce compromises the contributors may want to avoid
* The variance in tags are essentially just concentrated in a separate tag, not eliminated

