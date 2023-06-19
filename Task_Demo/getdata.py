import requests

#Creating Dictionary
def get_api_data(api_link):
    
    response = requests.get(api_link)
    
    
    if response.status_code == 200:  # Check if the request was successful
        return response.json()  # Assuming the API response is in JSON format
    # Process the data as needed
    else:
        print('API request failed with status code:', response.status_code)
    

urls = {
        
        "appointment":'https://xloop-dummy.herokuapp.com/appointment' ,
        "patient_councillor":'https://xloop-dummy.herokuapp.com/patient_councillor',
        "councillor":'https://xloop-dummy.herokuapp.com/councillor' ,
        "rating":   'https://xloop-dummy.herokuapp.com/rating'
    }