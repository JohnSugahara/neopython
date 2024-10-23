from neo4j import GraphDatabase

class GameDatabase:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_player(self, player_id, name):
        query = """
        MERGE (p:Player {id: $player_id})
        SET p.name = $name
        RETURN p
        """
        with self.driver.session() as session:
            result = session.run(query, player_id=player_id, name=name)
            return result.single()

    def update_player(self, player_id, name):
        query = """
        MATCH (p:Player {id: $player_id})
        SET p.name = $name
        RETURN p
        """
        with self.driver.session() as session:
            result = session.run(query, player_id=player_id, name=name)
            return result.single()

    def delete_player(self, player_id):
        query = """
        MATCH (p:Player {id: $player_id})
        DETACH DELETE p
        """
        with self.driver.session() as session:
            session.run(query, player_id=player_id)

    def create_match(self, match_id, player_ids, result):
        query = """
        MERGE (m:Match {id: $match_id})
        SET m.result = $result
        WITH m
        MATCH (p:Player)
        WHERE p.id IN $player_ids
        MERGE (p)-[:PLAYED]->(m)
        RETURN m
        """
        with self.driver.session() as session:
            result = session.run(query, match_id=match_id, player_ids=player_ids, result=result)
            return result.single()

    def update_match_result(self, match_id, result):
        query = """
        MATCH (m:Match {id: $match_id})
        SET m.result = $result
        RETURN m
        """
        with self.driver.session() as session:
            result = session.run(query, match_id=match_id, result=result)
            return result.single()

    def delete_match(self, match_id):
        query = """
        MATCH (m:Match {id: $match_id})
        DETACH DELETE m
        """
        with self.driver.session() as session:
            session.run(query, match_id=match_id)

    def get_player(self, player_id):
        query = """
        MATCH (p:Player {id: $player_id})
        RETURN p
        """
        with self.driver.session() as session:
            result = session.run(query, player_id=player_id)
            return result.single()

    def get_players(self):
        query = """
        MATCH (p:Player)
        RETURN p
        """
        with self.driver.session() as session:
            result = session.run(query)
            return result.values()

    def get_match(self, match_id):
        query = """
        MATCH (m:Match {id: $match_id})
        RETURN m
        """
        with self.driver.session() as session:
            result = session.run(query, match_id=match_id)
            return result.single()

    def get_matches_by_player(self, player_id):
        query = """
        MATCH (p:Player {id: $player_id})-[:PLAYED]->(m:Match)
        RETURN m
        """
        with self.driver.session() as session:
            result = session.run(query, player_id=player_id)
            return result.values()
