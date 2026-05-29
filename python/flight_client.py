import pyarrow.flight as flight
import pyarrow as pa
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlightClient:
    def __init__(self, host='flight-server', port=8815):
        self.location = flight.Location.for_grpc_tcp(host, port)
        self.client = None

    def connect(self):
        try:
            self.client = flight.FlightClient(self.location)
            logger.info(f"Connected to Flight server at {self.location}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Flight server: {e}")
            return False

    def fetch_data(self):
        if not self.client:
            logger.error("Client not connected")
            return None

        try:
            ticket = flight.Ticket(b"aggregated_data")
            reader = self.client.do_get(ticket)

            table = reader.read_all()
            logger.info(f"Received {len(table)} rows from Flight server")

            return table
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return None

    def process_data(self, table):
        if table is None or len(table) == 0:
            logger.warning("No data to process")
            return

        df = table.to_pandas()

        logger.info("\n=== Flight Data Analysis ===")
        logger.info(f"Total rows: {len(df)}")
        logger.info(f"Unique collectors: {df['collector_id'].nunique()}")
        logger.info(f"Unique meters: {df['meter_id'].nunique()}")
        logger.info(f"Total energy (sum): {df['sum_kwh'].sum():.2f} kWh")
        logger.info(f"Average power: {df['avg_kwh'].mean():.2f} kWh")
        logger.info(f"Min power: {df['min_kwh'].min():.2f} kWh")
        logger.info(f"Max power: {df['max_kwh'].max():.2f} kWh")
        logger.info(f"Average voltage: {df['avg_voltage'].mean():.2f} V")
        logger.info(f"Average current: {df['avg_current'].mean():.2f} A")
        logger.info("===========================\n")

        by_collector = df.groupby('collector_id').agg({
            'sum_kwh': 'sum',
            'count': 'sum',
            'meter_id': 'nunique'
        })

        logger.info("Per-collector statistics:")
        for collector_id, row in by_collector.iterrows():
            logger.info(f"  {collector_id}: {row['sum_kwh']:.2f} kWh, "
                       f"{row['count']} readings, {row['meter_id']} meters")

def main():
    client = FlightClient()

    while not client.connect():
        logger.info("Retrying connection in 5 seconds...")
        time.sleep(5)

    logger.info("Starting data fetch loop (every 30 seconds)...")

    while True:
        try:
            table = client.fetch_data()
            if table:
                client.process_data(table)
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
