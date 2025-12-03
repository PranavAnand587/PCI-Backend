try:
    import pandas as pd
    import geopandas as gpd
    import matplotlib.pyplot as plt
    from wordcloud import WordCloud
    from rapidfuzz import process, fuzz
    print("All libraries found")
except ImportError as e:
    print(f"Missing library: {e}")
