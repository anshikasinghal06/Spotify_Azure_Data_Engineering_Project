class reusable:
    def dropColumns(self, df, column):
        df = df.drop(*column)
        return df
    
    def dropDuplicates(self, df, column):
        df = df.dropDuplicates(column)
        return df
    