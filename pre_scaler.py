class PreScaler:

    """Class which essentially annihilates any entries where their chance of
    playing is zero, or they have no games in the next gameweek. Also scales up
    entries where they have > 1 game."""

    def fit(self, X, y):
        return self

    def transform(self, X):
        X = X.mul(X['chance_of_playing'].fillna(100)/100, axis=0)
        return X
