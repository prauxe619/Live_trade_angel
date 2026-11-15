from .shared_store import init_schema, read_snapshot

def main():
    init_schema()
    print("database_schema=ok")
    for symbol in ("NIFTY","BANKNIFTY","FINNIFTY","SENSEX"):
        s=read_snapshot(symbol, max_age_s=60)
        print(f"{symbol}: {'fresh' if s and not s.get('_stale') else 'missing_or_stale'}")
if __name__ == '__main__': main()
