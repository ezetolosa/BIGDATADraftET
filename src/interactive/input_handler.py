class UserInput:
    def __init__(self, data_manager):
        self.data_manager = data_manager
    
    def get_league_selection(self):
        available_leagues = self.data_manager.get_available_leagues()
        
        print("\nAvailable Leagues:")
        for idx, league in enumerate(available_leagues, 1):
            print(f"{idx}. {league}")
            
        while True:
            try:
                choice = int(input("\nSelect league number: "))
                if 1 <= choice <= len(available_leagues):
                    return available_leagues[choice-1]
                print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
    
    def get_team_selection(self, league, prompt):
        teams = self.data_manager.get_teams_in_league(league)
        
        print(f"\n{prompt}")
        for idx, team in enumerate(teams, 1):
            print(f"{idx}. {team}")
            
        while True:
            try:
                choice = int(input("\nSelect team number: "))
                if 1 <= choice <= len(teams):
                    return teams[choice-1]
                print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")