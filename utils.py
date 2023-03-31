from random_user_agent.user_agent import UserAgent
import time

def generate_header():
    headers = {
        'authority': 'statusinvest.com.br',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'user-agent': '',
    }
    #user_agent = random.choice(user_agents)  
    user_agent_rotator = UserAgent(limit=100)
    user_agent = user_agent_rotator.get_random_user_agent()
    headers['user-agent'] = user_agent
    return headers

def is_empty(obj):
    if obj.__class__.__module__ == 'builtins':
        return not obj
    elif 'pandas' in  obj.__class__.__module__:
        return obj.empty
    else:
        raise Exception('Type not defined in is_empty function')
    
def try_get_function(func: callable):
    def wrapper(*args, **kargs):
        limit = 5
        response = {}
        backoff = 2
        attempt = 1
        while (is_empty(response) and attempt <= limit):
            if attempt > 1 : print(f"Attempt {attempt} in {func.__doc__.strip()}")
            time.sleep(attempt*backoff)
            response = func(*args, **kargs)
            attempt += 1
        return response
    return wrapper