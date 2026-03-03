from src.extract import load_data
from src.transform import transform_all
from src.analytics import run_analytics
from src.load import run_load

def main():

    # EXTRACT
    dfs = load_data()

    # TRANSFORM
    dfs_clean = transform_all(dfs)

    #ANALYTICS
    analytics = run_analytics(dfs_clean)

    # LOAD
    run_load(dfs_clean, analytics)

    print('Pipeline corrido exitosamente!')


if __name__ == '__main__':
    main()
