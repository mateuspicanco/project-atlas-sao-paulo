# Project Atlas - São Paulo
**Project Atlas - São Paulo** is a Data Science and Engineering initiative that aims at developing *relevant and curated Geospatial features* about the city of São Paulo, Brazil. It's ultimate use is varied, but it is mainly focused on Machine Learning tasks, such as Real State price prediction.

It aggregates several attributes from many public data sources at different *levels of interest*, which can be used to match geospatially referenced data (`lat`,`long` pairs for example).

A breakdown of the data sources currently used and their original references can be found below, but the official documentation of the project contains the full list of data sources.

# Data sources
- [GeoSampa](http://geosampa.prefeitura.sp.gov.br/PaginasPublicas/_SBC.aspx): geospatial data exploration tool provided by the São Paulo's Department of Urban Development;
- [IBGE](https://downloads.ibge.gov.br/): raw datasets from the 2010 Census conducted by the Brazilian Institute of Geography and Statistics;
- [Infocidade](http://infocidade.prefeitura.sp.gov.br/): multiple sources from the city of São Paulo's local government entities;
- [São Paulo Open Data Portal](http://dados.prefeitura.sp.gov.br/pt_PT/): multiple curated data sources from the city of São Paulo;

## Relevant files:

1. `tb_district.parquet`: the dataset with all derived features aggregated at the District level;
2. `tb_neighborhood.parquet`: the dataset with all derived features aggregated at the Neighborhood level;
3. `tb_zipcode.parquet`: the dataset with all derived features aggregated at the Zipcode level;
4. `tb_area_of_ponderation`: the dataset with all derived features aggregated at the Area of Ponderation level;


<!-- ### Acknowledgements

We wouldn't be here without the help of others. If you owe any attributions or thanks, include them here along with any citations of past research. -->


## Inspiration

This project had various inspirations, such as the [Boston Housing Dataset](https://www.cs.toronto.edu/~delve/data/boston/bostonDetail.html). While I was studying relevant features for the real state market, I noticed that the classic Boston Housing dataset included several sociodemographic variables, which gave me the idea to do the same for São Paulo using the Brazilian Census data.