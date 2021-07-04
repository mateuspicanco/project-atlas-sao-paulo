# :earth_americas:Project Atlas - São Paulo
Geospatial features designed and built by [@mateuspicanco](mailto:mlimagom@gmail.com).

# Project Description
**Project Atlas - São Paulo** is a Data Science and Engineering initiative that aims at developing *relevant and curated Geospatial features* about the city of São Paulo, Brazil. It's ultimate use is varied, but it is mainly focused on Machine Learning tasks, such as Real State price prediction.s 

It aggregates several attributes from many public data sources at different *levels of interest*, which can be used to match geospatially referenced data (`lat`,`long` pairs for example).

A breakdown of the data sources currently used and their original references can be found below, but the official documentation of the project contains the full breakdowns.

# Data sources
- [GeoSampa](http://geosampa.prefeitura.sp.gov.br/PaginasPublicas/_SBC.aspx): geospatial data exploration tool provided by the São Paulo's Department of Urban Development;
- [IBGE](https://downloads.ibge.gov.br/): raw datasets from the 2010 Census conducted by the Brazilian Institute of Geography and Statistics;
- [Infocidade](http://infocidade.prefeitura.sp.gov.br/): multiple sources from the city of São Paulo's local government entities;

# Technologies used
The main technologies used in this project were:
1. Data processing: `Apache Spark`, `pyspark`;
2. Geospatial data wrangling: `Apache Sedona`, `geopandas`, `fiona`;
3. Storage and Metadata: `AWS S3`, `AWS Glue`, `AWS CloudFormation`;

# Project Architecture 

The project is broken down by different levels of granularities of Geospatial references. These are:

1. Census Sectors (Street blocks)
2. Streets (Zip code) 
3. Area of Ponderation (aggregation of sectors)
4. Neighborhoods
5. Districts