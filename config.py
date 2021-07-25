import pytz

ABOUT_APP_TEXT = """This app has been developed by [Lukas Calmbach](mailto:lcalmbach@gmail.com) using 
        [Python](https://www.python.org/), [Streamlit](https://streamlit.io/) and [Altair]
        (https://altair-viz.github.io/). All sourcecode is published on [github](https://github.com/lcalmbach/ede).
        """
IMG = {'main':'./static/images/app_welcome.jpg',
        '1': './static/images/pwqmn_about.png'
}
APP_WELCOME_TEXT = """edEx allows exploring and discovering environmental data. The application is mainly aimed at the analysis of surface and groundwater quality, however most options are useful for other media as well. Data from different sources my be combined in a project, for example water quality, stream flow and temperature. All available project are listed below. Select a project at the bottom of the list and click the `Open` button.
"""
LOGO_REFERENCE: str = 'https://github.com/lcalmbach/ede/blob/main/static/images/flask.png?raw=true'
USER_MANUAL_LINK: str = 'https://ede.readthedocs.io/en/latest/'
HELP_ICON: str = 'https://img.icons8.com/offices/30/000000/help.png'

default_plot_width = 800
default_plot_height = 600

MAP_LEGEND_SYMBOL_SIZE: int = 10
MAPBOX_STYLE: str = "mapbox://styles/mapbox/light-v10"
GRADIENT: str = 'blue-green'
TOOLTIP_FONTSIZE = 'x-small'
TOOLTIP_BACKCOLOR = 'white'
TOOLTIP_FORECOLOR = 'black'

tz_GMT = pytz.timezone('Europe/London')

MONTHS_DICT = {1:'Jan',2:'Feb',3:'Mrz',4:'Apr',5:'Mai',6:'Jun',7:'Jul',8:'Aug',9:'Sep',10:'Okt',11:'Nov',12:'Dez',}
MONTHS_REV_DICT = {'Jan': 1,'Feb':2,'Mrz':3,'Apr':4,'Mai':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Okt':10,'Nov':11,'Dez':12,}