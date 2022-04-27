import requests
class MetaWeatherApi:
    """This class has the methods for each endpoints in the meta weather api"""

    def __init__(self,logger_obj) -> None:
        """This is the init method of the class MeataWeatherApi"""
        self.logging = logger_obj
        self.logging.info("Object created successfully for the class MetaWeatherApi")

    def weather_information_using_woeid_date(self, woeid, date):
        """This method will provide the weather information based on woeid and date
        which give as paramether at endpoint  /api/location/(woeid)/(date)/"""
        try:
            endpoint = "https://www.metaweather.com/api/location/{woeid}/{date}/".format(woeid=woeid,date=date)
            response = requests.get(endpoint).json()
            self.logging.info("Weather information sucessfully retrieved for{date} and {woeid}".format(date=date,woeid=woeid))
        except Exception as err:
            print(err)
            response = None
        return response