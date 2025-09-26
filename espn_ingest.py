# nba_live_api.py
"""
NBA Live Game Data API Structure
Similar to nba_api.live but customized for our needs
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
import json
from enum import Enum

class GameStatus(Enum):
    """Game status enumeration"""
    SCHEDULED = 1
    LIVE = 2
    FINAL = 3
    POSTPONED = 4

class PeriodType(Enum):
    """Period type enumeration"""
    REGULAR = "REGULAR"
    OVERTIME = "OVERTIME"

@dataclass
class Arena:
    """Arena information"""
    arena_id: int
    arena_name: str
    arena_city: str
    arena_state: str
    arena_country: str
    arena_timezone: str
    
    def to_dict(self) -> Dict:
        return {
            "arenaId": self.arena_id,
            "arenaName": self.arena_name,
            "arenaCity": self.arena_city,
            "arenaState": self.arena_state,
            "arenaCountry": self.arena_country,
            "arenaTimezone": self.arena_timezone
        }

@dataclass
class Official:
    """Game official information"""
    person_id: int
    name: str
    name_i: str
    first_name: str
    family_name: str
    jersey_num: str
    assignment: str
    
    def to_dict(self) -> Dict:
        return {
            "personId": self.person_id,
            "name": self.name,
            "nameI": self.name_i,
            "firstName": self.first_name,
            "familyName": self.family_name,
            "jerseyNum": self.jersey_num,
            "assignment": self.assignment
        }

@dataclass
class PlayerStatistics:
    """Player game statistics"""
    minutes: str = "PT00M00.00S"
    seconds: int = 0
    points: int = 0
    assists: int = 0
    rebounds: int = 0
    rebounds_defensive: int = 0
    rebounds_offensive: int = 0
    steals: int = 0
    blocks: int = 0
    turnovers: int = 0
    field_goals_made: int = 0
    field_goals_attempted: int = 0
    field_goals_percentage: float = 0.0
    three_pointers_made: int = 0
    three_pointers_attempted: int = 0
    three_pointers_percentage: float = 0.0
    free_throws_made: int = 0
    free_throws_attempted: int = 0
    free_throws_percentage: float = 0.0
    fouls_personal: int = 0
    fouls_technical: int = 0
    fouls_offensive: int = 0
    fouls_drawn: int = 0
    plus_minus: int = 0
    
    def to_dict(self) -> Dict:
        return {
            "minutes": self.minutes,
            "seconds": self.seconds,
            "points": self.points,
            "assists": self.assists,
            "reboundsTotal": self.rebounds,
            "reboundsDefensive": self.rebounds_defensive,
            "reboundsOffensive": self.rebounds_offensive,
            "steals": self.steals,
            "blocks": self.blocks,
            "turnovers": self.turnovers,
            "fieldGoalsMade": self.field_goals_made,
            "fieldGoalsAttempted": self.field_goals_attempted,
            "fieldGoalsPercentage": self.field_goals_percentage,
            "threePointersMade": self.three_pointers_made,
            "threePointersAttempted": self.three_pointers_attempted,
            "threePointersPercentage": self.three_pointers_percentage,
            "freeThrowsMade": self.free_throws_made,
            "freeThrowsAttempted": self.free_throws_attempted,
            "freeThrowsPercentage": self.free_throws_percentage,
            "foulsPersonal": self.fouls_personal,
            "foulsTechnical": self.fouls_technical,
            "foulsOffensive": self.fouls_offensive,
            "foulsDrawn": self.fouls_drawn,
            "plusMinusPoints": self.plus_minus
        }

@dataclass
class Player:
    """Player information with statistics"""
    person_id: int
    name: str
    name_i: str
    first_name: str
    family_name: str
    jersey_num: str
    position: str
    status: str = "ACTIVE"
    order: int = 0
    starter: str = "0"
    oncourt: str = "0"
    played: str = "0"
    statistics: PlayerStatistics = field(default_factory=PlayerStatistics)
    
    def to_dict(self) -> Dict:
        return {
            "status": self.status,
            "order": self.order,
            "personId": self.person_id,
            "name": self.name,
            "nameI": self.name_i,
            "firstName": self.first_name,
            "familyName": self.family_name,
            "jerseyNum": self.jersey_num,
            "position": self.position,
            "starter": self.starter,
            "oncourt": self.oncourt,
            "played": self.played,
            "statistics": self.statistics.to_dict()
        }

@dataclass
class Period:
    """Period/Quarter information"""
    period: int
    period_type: PeriodType
    score: int
    
    def to_dict(self) -> Dict:
        return {
            "period": self.period,
            "periodType": self.period_type.value,
            "score": self.score
        }

@dataclass
class TeamStatistics:
    """Team game statistics"""
    points: int = 0
    field_goals_made: int = 0
    field_goals_attempted: int = 0
    field_goals_percentage: float = 0.0
    three_pointers_made: int = 0
    three_pointers_attempted: int = 0
    three_pointers_percentage: float = 0.0
    free_throws_made: int = 0
    free_throws_attempted: int = 0
    free_throws_percentage: float = 0.0
    rebounds_total: int = 0
    rebounds_offensive: int = 0
    rebounds_defensive: int = 0
    assists: int = 0
    steals: int = 0
    blocks: int = 0
    turnovers: int = 0
    fouls_personal: int = 0
    fouls_technical: int = 0
    
    def to_dict(self) -> Dict:
        return {
            "points": self.points,
            "fieldGoalsMade": self.field_goals_made,
            "fieldGoalsAttempted": self.field_goals_attempted,
            "fieldGoalsPercentage": self.field_goals_percentage,
            "threePointersMade": self.three_pointers_made,
            "threePointersAttempted": self.three_pointers_attempted,
            "threePointersPercentage": self.three_pointers_percentage,
            "freeThrowsMade": self.free_throws_made,
            "freeThrowsAttempted": self.free_throws_attempted,
            "freeThrowsPercentage": self.free_throws_percentage,
            "reboundsTotal": self.rebounds_total,
            "reboundsOffensive": self.rebounds_offensive,
            "reboundsDefensive": self.rebounds_defensive,
            "assists": self.assists,
            "steals": self.steals,
            "blocks": self.blocks,
            "turnovers": self.turnovers,
            "foulsPersonal": self.fouls_personal,
            "foulsTechnical": self.fouls_technical
        }

@dataclass
class Team:
    """Team information with players and statistics"""
    team_id: int
    team_name: str
    team_city: str
    team_tricode: str
    score: int = 0
    in_bonus: str = "0"
    timeouts_remaining: int = 7
    periods: List[Period] = field(default_factory=list)
    players: List[Player] = field(default_factory=list)
    statistics: TeamStatistics = field(default_factory=TeamStatistics)
    
    def to_dict(self) -> Dict:
        return {
            "teamId": self.team_id,
            "teamName": self.team_name,
            "teamCity": self.team_city,
            "teamTricode": self.team_tricode,
            "score": self.score,
            "inBonus": self.in_bonus,
            "timeoutsRemaining": self.timeouts_remaining,
            "periods": [p.to_dict() for p in self.periods],
            "players": [p.to_dict() for p in self.players],
            "statistics": self.statistics.to_dict()
        }

@dataclass
class Game:
    """Complete game information"""
    game_id: str
    game_code: str
    game_status: GameStatus
    game_status_text: str
    period: int
    game_clock: str
    game_time_utc: datetime
    game_time_local: datetime
    game_time_home: datetime
    game_time_away: datetime
    game_time_et: datetime
    duration: int = 0
    regulation_periods: int = 4
    attendance: int = 0
    sellout: str = "0"
    arena: Optional[Arena] = None
    officials: List[Official] = field(default_factory=list)
    home_team: Optional[Team] = None
    away_team: Optional[Team] = None
    
    def to_dict(self) -> Dict:
        return {
            "gameId": self.game_id,
            "gameCode": self.game_code,
            "gameStatus": self.game_status.value,
            "gameStatusText": self.game_status_text,
            "period": self.period,
            "gameClock": self.game_clock,
            "gameTimeUTC": self.game_time_utc.isoformat() + "Z",
            "gameTimeLocal": self.game_time_local.isoformat(),
            "gameTimeHome": self.game_time_home.isoformat(),
            "gameTimeAway": self.game_time_away.isoformat(),
            "gameEt": self.game_time_et.isoformat(),
            "duration": self.duration,
            "regulationPeriods": self.regulation_periods,
            "attendance": self.attendance,
            "sellout": self.sellout,
            "arena": self.arena.to_dict() if self.arena else None,
            "officials": [o.to_dict() for o in self.officials],
            "homeTeam": self.home_team.to_dict() if self.home_team else None,
            "awayTeam": self.away_team.to_dict() if self.away_team else None
        }

@dataclass
class BoxScore:
    """Complete box score response"""
    meta: Dict[str, Any]
    game: Game
    
    def to_dict(self) -> Dict:
        return {
            "meta": self.meta,
            "game": self.game.to_dict()
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2, default=str)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'BoxScore':
        """Create BoxScore from dictionary"""
        # Parse meta
        meta = data.get("meta", {})
        
        # Parse game data
        game_data = data.get("game", {})
        
        # Parse arena
        arena_data = game_data.get("arena")
        arena = None
        if arena_data:
            arena = Arena(
                arena_id=arena_data.get("arenaId"),
                arena_name=arena_data.get("arenaName"),
                arena_city=arena_data.get("arenaCity"),
                arena_state=arena_data.get("arenaState"),
                arena_country=arena_data.get("arenaCountry"),
                arena_timezone=arena_data.get("arenaTimezone")
            )
        
        # Parse officials
        officials = []
        for off_data in game_data.get("officials", []):
            officials.append(Official(
                person_id=off_data.get("personId"),
                name=off_data.get("name"),
                name_i=off_data.get("nameI"),
                first_name=off_data.get("firstName"),
                family_name=off_data.get("familyName"),
                jersey_num=off_data.get("jerseyNum"),
                assignment=off_data.get("assignment")
            ))
        
        # Parse teams
        home_team = cls._parse_team(game_data.get("homeTeam"))
        away_team = cls._parse_team(game_data.get("awayTeam"))
        
        # Create game
        game = Game(
            game_id=game_data.get("gameId"),
            game_code=game_data.get("gameCode"),
            game_status=GameStatus(game_data.get("gameStatus", 1)),
            game_status_text=game_data.get("gameStatusText"),
            period=game_data.get("period"),
            game_clock=game_data.get("gameClock"),
            game_time_utc=datetime.fromisoformat(game_data.get("gameTimeUTC", "").replace("Z", "+00:00")),
            game_time_local=datetime.fromisoformat(game_data.get("gameTimeLocal", "")),
            game_time_home=datetime.fromisoformat(game_data.get("gameTimeHome", "")),
            game_time_away=datetime.fromisoformat(game_data.get("gameTimeAway", "")),
            game_time_et=datetime.fromisoformat(game_data.get("gameEt", "")),
            duration=game_data.get("duration", 0),
            regulation_periods=game_data.get("regulationPeriods", 4),
            attendance=game_data.get("attendance", 0),
            sellout=game_data.get("sellout", "0"),
            arena=arena,
            officials=officials,
            home_team=home_team,
            away_team=away_team
        )
        
        return cls(meta=meta, game=game)
    
    @staticmethod
    def _parse_team(team_data: Optional[Dict]) -> Optional[Team]:
        """Parse team data from dictionary"""
        if not team_data:
            return None
        
        # Parse periods
        periods = []
        for period_data in team_data.get("periods", []):
            periods.append(Period(
                period=period_data.get("period"),
                period_type=PeriodType(period_data.get("periodType", "REGULAR")),
                score=period_data.get("score")
            ))
        
        # Parse players
        players = []
        for player_data in team_data.get("players", []):
            stats_data = player_data.get("statistics", {})
            stats = PlayerStatistics(
                minutes=stats_data.get("minutes", "PT00M00.00S"),
                seconds=stats_data.get("seconds", 0),
                points=stats_data.get("points", 0),
                assists=stats_data.get("assists", 0),
                rebounds=stats_data.get("reboundsTotal", 0),
                rebounds_defensive=stats_data.get("reboundsDefensive", 0),
                rebounds_offensive=stats_data.get("reboundsOffensive", 0),
                steals=stats_data.get("steals", 0),
                blocks=stats_data.get("blocks", 0),
                turnovers=stats_data.get("turnovers", 0),
                field_goals_made=stats_data.get("fieldGoalsMade", 0),
                field_goals_attempted=stats_data.get("fieldGoalsAttempted", 0),
                field_goals_percentage=stats_data.get("fieldGoalsPercentage", 0.0),
                three_pointers_made=stats_data.get("threePointersMade", 0),
                three_pointers_attempted=stats_data.get("threePointersAttempted", 0),
                three_pointers_percentage=stats_data.get("threePointersPercentage", 0.0),
                free_throws_made=stats_data.get("freeThrowsMade", 0),
                free_throws_attempted=stats_data.get("freeThrowsAttempted", 0),
                free_throws_percentage=stats_data.get("freeThrowsPercentage", 0.0),
                fouls_personal=stats_data.get("foulsPersonal", 0),
                fouls_technical=stats_data.get("foulsTechnical", 0),
                fouls_offensive=stats_data.get("foulsOffensive", 0),
                fouls_drawn=stats_data.get("foulsDrawn", 0),
                plus_minus=stats_data.get("plusMinusPoints", 0)
            )
            
            players.append(Player(
                person_id=player_data.get("personId"),
                name=player_data.get("name", ""),
                name_i=player_data.get("nameI", ""),
                first_name=player_data.get("firstName", ""),
                family_name=player_data.get("familyName", ""),
                jersey_num=player_data.get("jerseyNum", ""),
                position=player_data.get("position", ""),
                status=player_data.get("status", "ACTIVE"),
                order=player_data.get("order", 0),
                starter=player_data.get("starter", "0"),
                oncourt=player_data.get("oncourt", "0"),
                played=player_data.get("played", "0"),
                statistics=stats
            ))
        
        # Parse team statistics
        team_stats_data = team_data.get("statistics", {})
        team_stats = TeamStatistics(
            points=team_stats_data.get("points", 0),
            field_goals_made=team_stats_data.get("fieldGoalsMade", 0),
            field_goals_attempted=team_stats_data.get("fieldGoalsAttempted", 0),
            field_goals_percentage=team_stats_data.get("fieldGoalsPercentage", 0.0),
            three_pointers_made=team_stats_data.get("threePointersMade", 0),
            three_pointers_attempted=team_stats_data.get("threePointersAttempted", 0),
            three_pointers_percentage=team_stats_data.get("threePointersPercentage", 0.0),
            free_throws_made=team_stats_data.get("freeThrowsMade", 0),
            free_throws_attempted=team_stats_data.get("freeThrowsAttempted", 0),
            free_throws_percentage=team_stats_data.get("freeThrowsPercentage", 0.0),
            rebounds_total=team_stats_data.get("reboundsTotal", 0),
            rebounds_offensive=team_stats_data.get("reboundsOffensive", 0),
            rebounds_defensive=team_stats_data.get("reboundsDefensive", 0),
            assists=team_stats_data.get("assists", 0),
            steals=team_stats_data.get("steals", 0),
            blocks=team_stats_data.get("blocks", 0),
            turnovers=team_stats_data.get("turnovers", 0),
            fouls_personal=team_stats_data.get("foulsPersonal", 0),
            fouls_technical=team_stats_data.get("foulsTechnical", 0)
        )
        
        return Team(
            team_id=team_data.get("teamId"),
            team_name=team_data.get("teamName"),
            team_city=team_data.get("teamCity"),
            team_tricode=team_data.get("teamTricode"),
            score=team_data.get("score", 0),
            in_bonus=team_data.get("inBonus", "0"),
            timeouts_remaining=team_data.get("timeoutsRemaining", 7),
            periods=periods,
            players=players,
            statistics=team_stats
        )


# Example usage
if __name__ == "__main__":
    # Create sample data
    sample_arena = Arena(
        arena_id=17,
        arena_name="TD Garden",
        arena_city="Boston",
        arena_state="MA",
        arena_country="US",
        arena_timezone="America/New_York"
    )
    
    sample_official = Official(
        person_id=201638,
        name="Brent Barnaky",
        name_i="B. Barnaky",
        first_name="Brent",
        family_name="Barnaky",
        jersey_num="36",
        assignment="OFFICIAL1"
    )
    
    sample_player_stats = PlayerStatistics(
        minutes="PT32M45.00S",
        seconds=1965,
        points=25,
        assists=8,
        rebounds=7,
        field_goals_made=9,
        field_goals_attempted=18,
        field_goals_percentage=0.5,
        three_pointers_made=3,
        three_pointers_attempted=8,
        three_pointers_percentage=0.375,
        free_throws_made=4,
        free_throws_attempted=4,
        free_throws_percentage=1.0
    )
    
    sample_player = Player(
        person_id=1627759,
        name="Jaylen Brown",
        name_i="J. Brown",
        first_name="Jaylen",
        family_name="Brown",
        jersey_num="7",
        position="SF",
        starter="1",
        played="1",
        statistics=sample_player_stats
    )
    
    sample_period = Period(
        period=1,
        period_type=PeriodType.REGULAR,
        score=34
    )
    
    sample_team = Team(
        team_id=1610612738,
        team_name="Celtics",
        team_city="Boston",
        team_tricode="BOS",
        score=124,
        in_bonus="1",
        timeouts_remaining=2,
        periods=[sample_period],
        players=[sample_player]
    )
    
    sample_game = Game(
        game_id="0022000180",
        game_code="20210115/ORLBOS",
        game_status=GameStatus.FINAL,
        game_status_text="Final",
        period=4,
        game_clock="PT00M00.00S",
        game_time_utc=datetime.fromisoformat("2021-01-16T00:30:00+00:00"),
        game_time_local=datetime.fromisoformat("2021-01-15T19:30:00-05:00"),
        game_time_home=datetime.fromisoformat("2021-01-15T19:30:00-05:00"),
        game_time_away=datetime.fromisoformat("2021-01-15T19:30:00-05:00"),
        game_time_et=datetime.fromisoformat("2021-01-15T19:30:00-05:00"),
        duration=125,
        arena=sample_arena,
        officials=[sample_official],
        home_team=sample_team
    )
    
    sample_meta = {
        "version": 1,
        "code": 200,
        "request": "http://nba.cloud/games/0022000180/boxscore?Format=json",
        "time": "2021-01-15 23:51:25.282704"
    }
    
    box_score = BoxScore(meta=sample_meta, game=sample_game)
    
    # Convert to JSON
    print(box_score.to_json())
