# Digital Futures Data Engineering Capstone Project
## Sovereign Bond Yield Tracker: ETL Pipeline and Dashboard

Members of our Data Engineering cohort were tasked with building a basic ETL pipeline and associated Streamlit app that draws on the data we load into our DB.

This repo contains the Project Plan, ETL pipeline code, Streamlit dashboard code, and the presentation slideshow/video for the project.

# https://dashbunds.streamlit.app/

## Motivation and Aims

Going through a GE campaign at the time of being tasked with this project, I became curious about the relationship between a political party's ups and downs and any associated movements in the sovereign bond markets.

This pipeline serves as a launchpad for potentially investigating that analytically.

At present this pipeline and dashboard only handle bond yield data... 
By developing a second pipeline for handling political news articles/coverage one could investigate hypotheses regarding the relationship between the political events' favourability towards/against one party and how the markets are positioning themselves with respect to one party's potential victory/loss.

I had envisioned this as involving some degree of sentiment analysis on the news coverage to determine the "victory probability swing" on a day to day basis, and then a time series analysis against the bond yield data to make some kind of inference on the market sentiment towards the Labour/Conservative parties.
