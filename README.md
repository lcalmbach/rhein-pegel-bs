# rhein-pegel-bs

This app is written in Python and uses the frameworks [streamlit](https://streamlit.io) and [altair](https://altair-viz.github.io/). It downloads precipitation and Rhine-River flow data from [opendata.bs](https://https://data.bs.ch/pages/home/) and displays daily averaged values side by side for specified dates. An animation button steps through all dates until today and shows how heavy rains lead to an increase in the river flow and dry periods to a decrease in flow. 

The app is published [here](https://github.com/lcalmbach/rhein-pegel-bs). To run the code on your local machine follow these steps to install the required environment:
1. git clone https://github.com/lcalmbach/rhein-pegel-bs.git
2. create and activate a virtual environment and install the libraries, for example in Windows:
    ```
    > Python -m venv env
    > env\scripts\activate
    > (env) pip install -r requirements.txt
    > (env) pip install -r requirements.txt
    ```
1. run streamlit server locally by typing:
    ```
    > (env) streamlit run app.py
    ```
1. Open App in Browser on http://localhost:8501