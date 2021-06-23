# Project Atlas - São Paulo
Geospatial features designed and built by [@mateuspicanco](mailto:mlimagom@gmail.com).

# Description
Project Atlas - São Paulo is a Data Science and Engineering initiative to develop Geospatial features about the city of São Paulo, Brazil. It congregates several attributes from many public data sources, mostly made available by the [GeoSampa website](http://geosampa.prefeitura.sp.gov.br/PaginasPublicas/_SBC.aspx), a project by the São Paulo City Hall. 

A full breakdown of the data sources and their original references can be found below:

- [ ] Add list of data sources

# Technologies
The main technologies used in this project were:
1. Data processing: `Apache Spark`, `pyspark`;
2. Geospatial data wrangling: `Apache Sedona`;
3. Data Visualization: `folium`;
4. Storage and Metadata: `AWS S3`, `AWS Glue`, `AWS CloudFormation`;
5. Versioning and Tracking: `dvc`, `git`;

# Architecture 

The project is broken down by different levels of granularities of Geospatial references. These are:

1. Street / Zip Code level
2. Subdistrict (Neighborhood)
3. District
4. Macro Regions