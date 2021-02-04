import requests

class Player:

    def __init__(self, player_id):
        self.player_id = player_id
        player_data = requests.get("https://fantasy.premierleague.com/api/element-summary/{}/".format(
            self.player_id)).json()
        self.fixtures = player_data['fixtures'][0]
        try: 
            self.history = player_data['history'][9]
        except: 
            self.history = {"element":0,"fixture":0,"opponent_team":0,"total_points":0,"was_home":0,
            "kickoff_time":None,"team_h_score":None,"team_a_score":None,"round":None,"minutes":None,
            "goals_scored":None,"assists":None,"clean_sheets":None,"goals_conceded":0,
            "own_goals":0,"penalties_saved":0,"penalties_missed":0,"yellow_cards":1,"red_cards":0,
            "saves":0,"bonus":0,"bps":0,"influence":"0","creativity":"0","threat":"0","ict_index":"0",
            "value":0,"transfers_balance":0,"selected":0,"transfers_in":0,"transfers_out":0}
            for key in self.history.keys():
                self.history[key] = None