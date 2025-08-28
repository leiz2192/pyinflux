import argparse
import cmd

from tabulate import tabulate
from influxdb import InfluxDBClient

class Db:
    def __init__(self, host, port, user, password, ssl):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ssl = ssl
        self.db = ""
        self.rp = ""

    def set_dbrp(self, dbrp):
        items = dbrp.strip().split(".")
        self.db = items[0]
        self.rp = items[1] if len(items) > 1 else ""

    def query(self, query):
        client = InfluxDBClient(self.host, self.port, self.user, self.password, database=self.db, ssl=self.ssl)
        return client.query(query).raw

    def write(self, data):
        points = data.split("|")
        client = InfluxDBClient(self.host, self.port, self.user, self.password, database=self.db, ssl=self.ssl)
        return client.write_points(points, database=self.db, retention_policy=self.rp, protocol="line")


class Cli(cmd.Cmd):
    prompt = "\n> "
    def __init__(self, db):
        cmd.Cmd.__init__(self)
        self.db = db

    def do_insert(self, line):
        try:
            self.db.write(line)
        except Exception as e:
            print(e)

    def do_use(self, line):
        self.db.set_dbrp(line)

    def default(self, line):
        try:
            resp = self.db.query(line)
        except Exception as e:
            print(e)
            return

        for series in resp.get('series', []):
            name = series.get('name')
            if name:
                print("name: {}".format(name))
            tags = series.get('tags')
            if tags:
                items = ["{}={}".format(key, value) for key, value in tags.items()]
                print("tags: {}".format(", ".join(items)))
            print(tabulate(series.get("values", []), headers=series.get("columns", [])))
            print()

    def do_exit(self, arg):
        return True

    def do_quit(self, arg):
        return True

def main():
    parser = argparse.ArgumentParser("InfluxDB shell version v0.0.1")
    parser.add_argument('-host', default="localhost", help="Influxdb host to connect to.")
    parser.add_argument('-port', default=8086, help="Influxdb port to connect to.")
    parser.add_argument('-user', help="Username to connect to the server.")
    parser.add_argument('-password', help="Password to connect to the server.")
    parser.add_argument('-ssl', action="store_true", help="Use https for connecting to cluster.")
    args = parser.parse_args()
    db = Db(args.host, args.port, args.user, args.password, args.ssl)
    Cli(db).cmdloop()

if __name__ == "__main__":
    main()