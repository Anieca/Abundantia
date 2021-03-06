from enum import Enum, auto

from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class BinanceKline:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    quote_asset_volume: float
    number_of_trade: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float
    ignore: str


class BinanceSymbols(Enum):
    BTCUSDT = auto()
    ETHUSDT = auto()
    BNBUSDT = auto()
    NEOUSDT = auto()
    LTCUSDT = auto()
    QTUMUSDT = auto()
    ADAUSDT = auto()
    XRPUSDT = auto()
    EOSUSDT = auto()
    TUSDUSDT = auto()
    IOTAUSDT = auto()
    XLMUSDT = auto()
    ONTUSDT = auto()
    TRXUSDT = auto()
    ETCUSDT = auto()
    ICXUSDT = auto()
    NULSUSDT = auto()
    VETUSDT = auto()
    USDCUSDT = auto()
    LINKUSDT = auto()
    WAVESUSDT = auto()
    ONGUSDT = auto()
    HOTUSDT = auto()
    ZILUSDT = auto()
    ZRXUSDT = auto()
    FETUSDT = auto()
    BATUSDT = auto()
    XMRUSDT = auto()
    ZECUSDT = auto()
    IOSTUSDT = auto()
    CELRUSDT = auto()
    DASHUSDT = auto()
    OMGUSDT = auto()
    THETAUSDT = auto()
    ENJUSDT = auto()
    MITHUSDT = auto()
    MATICUSDT = auto()
    ATOMUSDT = auto()
    TFUELUSDT = auto()
    ONEUSDT = auto()
    FTMUSDT = auto()
    ALGOUSDT = auto()
    GTOUSDT = auto()
    DOGEUSDT = auto()
    DUSKUSDT = auto()
    ANKRUSDT = auto()
    WINUSDT = auto()
    COSUSDT = auto()
    COCOSUSDT = auto()
    MTLUSDT = auto()
    TOMOUSDT = auto()
    PERLUSDT = auto()
    DENTUSDT = auto()
    MFTUSDT = auto()
    KEYUSDT = auto()
    DOCKUSDT = auto()
    WANUSDT = auto()
    FUNUSDT = auto()
    CVCUSDT = auto()
    CHZUSDT = auto()
    BANDUSDT = auto()
    BUSDUSDT = auto()
    BEAMUSDT = auto()
    XTZUSDT = auto()
    RENUSDT = auto()
    RVNUSDT = auto()
    HBARUSDT = auto()
    NKNUSDT = auto()
    STXUSDT = auto()
    KAVAUSDT = auto()
    ARPAUSDT = auto()
    IOTXUSDT = auto()
    RLCUSDT = auto()
    CTXCUSDT = auto()
    BCHUSDT = auto()
    TROYUSDT = auto()
    VITEUSDT = auto()
    FTTUSDT = auto()
    EURUSDT = auto()
    OGNUSDT = auto()
    DREPUSDT = auto()
    TCTUSDT = auto()
    WRXUSDT = auto()
    BTSUSDT = auto()
    LSKUSDT = auto()
    BNTUSDT = auto()
    LTOUSDT = auto()
    AIONUSDT = auto()
    MBLUSDT = auto()
    COTIUSDT = auto()
    STPTUSDT = auto()
    WTCUSDT = auto()
    DATAUSDT = auto()
    SOLUSDT = auto()
    CTSIUSDT = auto()
    HIVEUSDT = auto()
    CHRUSDT = auto()
    BTCUPUSDT = auto()
    BTCDOWNUSDT = auto()
    ARDRUSDT = auto()
    MDTUSDT = auto()
    STMXUSDT = auto()
    KNCUSDT = auto()
    REPUSDT = auto()
    LRCUSDT = auto()
    PNTUSDT = auto()
    COMPUSDT = auto()
    SCUSDT = auto()
    ZENUSDT = auto()
    SNXUSDT = auto()
    ETHUPUSDT = auto()
    ETHDOWNUSDT = auto()
    ADAUPUSDT = auto()
    ADADOWNUSDT = auto()
    LINKUPUSDT = auto()
    LINKDOWNUSDT = auto()
    VTHOUSDT = auto()
    DGBUSDT = auto()
    GBPUSDT = auto()
    SXPUSDT = auto()
    MKRUSDT = auto()
    DCRUSDT = auto()
    STORJUSDT = auto()
    BNBUPUSDT = auto()
    BNBDOWNUSDT = auto()
    MANAUSDT = auto()
    AUDUSDT = auto()
    YFIUSDT = auto()
    BALUSDT = auto()
    BLZUSDT = auto()
    IRISUSDT = auto()
    KMDUSDT = auto()
    JSTUSDT = auto()
    SRMUSDT = auto()
    ANTUSDT = auto()
    CRVUSDT = auto()
    SANDUSDT = auto()
    OCEANUSDT = auto()
    NMRUSDT = auto()
    DOTUSDT = auto()
    LUNAUSDT = auto()
    RSRUSDT = auto()
    PAXGUSDT = auto()
    WNXMUSDT = auto()
    TRBUSDT = auto()
    SUSHIUSDT = auto()
    YFIIUSDT = auto()
    KSMUSDT = auto()
    EGLDUSDT = auto()
    DIAUSDT = auto()
    RUNEUSDT = auto()
    FIOUSDT = auto()
    UMAUSDT = auto()
    TRXUPUSDT = auto()
    TRXDOWNUSDT = auto()
    XRPUPUSDT = auto()
    XRPDOWNUSDT = auto()
    DOTUPUSDT = auto()
    DOTDOWNUSDT = auto()
    BELUSDT = auto()
    WINGUSDT = auto()
    UNIUSDT = auto()
    NBSUSDT = auto()
    OXTUSDT = auto()
    SUNUSDT = auto()
    AVAXUSDT = auto()
    HNTUSDT = auto()
    FLMUSDT = auto()
    ORNUSDT = auto()
    UTKUSDT = auto()
    XVSUSDT = auto()
    ALPHAUSDT = auto()
    AAVEUSDT = auto()
    NEARUSDT = auto()
    FILUSDT = auto()
    INJUSDT = auto()
    AUDIOUSDT = auto()
    CTKUSDT = auto()
    AKROUSDT = auto()
    AXSUSDT = auto()
    HARDUSDT = auto()
    DNTUSDT = auto()
    STRAXUSDT = auto()
    UNFIUSDT = auto()
    ROSEUSDT = auto()
    AVAUSDT = auto()
    XEMUSDT = auto()
    SKLUSDT = auto()
    GRTUSDT = auto()
    JUVUSDT = auto()
    PSGUSDT = auto()
    REEFUSDT = auto()
    OGUSDT = auto()
    ATMUSDT = auto()
    ASRUSDT = auto()
    CELOUSDT = auto()
    RIFUSDT = auto()
    BTCSTUSDT = auto()
    TRUUSDT = auto()
    CKBUSDT = auto()
    TWTUSDT = auto()
    FIROUSDT = auto()
    LITUSDT = auto()
    SFPUSDT = auto()
    DODOUSDT = auto()
    CAKEUSDT = auto()
    ACMUSDT = auto()
    BADGERUSDT = auto()
    FISUSDT = auto()
    OMUSDT = auto()
    PONDUSDT = auto()
    DEGOUSDT = auto()
    ALICEUSDT = auto()
    LINAUSDT = auto()
    PERPUSDT = auto()
    RAMPUSDT = auto()
    SUPERUSDT = auto()
    CFXUSDT = auto()
    AUTOUSDT = auto()
    TKOUSDT = auto()
    PUNDIXUSDT = auto()
    TLMUSDT = auto()
    BTGUSDT = auto()
    MIRUSDT = auto()
    BARUSDT = auto()
    FORTHUSDT = auto()
    BAKEUSDT = auto()
    BURGERUSDT = auto()
    SLPUSDT = auto()
    SHIBUSDT = auto()
    ICPUSDT = auto()
    ARUSDT = auto()
    POLSUSDT = auto()
    MDXUSDT = auto()
    MASKUSDT = auto()
    LPTUSDT = auto()
    XVGUSDT = auto()
    ATAUSDT = auto()
    GTCUSDT = auto()
    TORNUSDT = auto()
    ERNUSDT = auto()
    KLAYUSDT = auto()
    PHAUSDT = auto()
    BONDUSDT = auto()
    MLNUSDT = auto()
    DEXEUSDT = auto()
    C98USDT = auto()
    CLVUSDT = auto()
    QNTUSDT = auto()
    FLOWUSDT = auto()
    TVKUSDT = auto()
    MINAUSDT = auto()
    RAYUSDT = auto()
    FARMUSDT = auto()
    ALPACAUSDT = auto()
    QUICKUSDT = auto()
    MBOXUSDT = auto()
    FORUSDT = auto()
    REQUSDT = auto()
    GHSTUSDT = auto()
    WAXPUSDT = auto()
    TRIBEUSDT = auto()
    GNOUSDT = auto()
    XECUSDT = auto()
    ELFUSDT = auto()
    DYDXUSDT = auto()
    POLYUSDT = auto()
    IDEXUSDT = auto()
    VIDTUSDT = auto()
    USDPUSDT = auto()
    GALAUSDT = auto()
    ILVUSDT = auto()
    YGGUSDT = auto()
    SYSUSDT = auto()
    DFUSDT = auto()
    FIDAUSDT = auto()
    FRONTUSDT = auto()
    CVPUSDT = auto()
    AGLDUSDT = auto()
    RADUSDT = auto()
    BETAUSDT = auto()
    RAREUSDT = auto()
    LAZIOUSDT = auto()
    CHESSUSDT = auto()
    ADXUSDT = auto()
    AUCTIONUSDT = auto()
    DARUSDT = auto()
    BNXUSDT = auto()
    MOVRUSDT = auto()
    CITYUSDT = auto()
    ENSUSDT = auto()
    KP3RUSDT = auto()
    QIUSDT = auto()
    PORTOUSDT = auto()
    POWRUSDT = auto()
    VGXUSDT = auto()
    JASMYUSDT = auto()
    AMPUSDT = auto()
    PLAUSDT = auto()
    PYRUSDT = auto()
    RNDRUSDT = auto()
    ALCXUSDT = auto()
    SANTOSUSDT = auto()
    MCUSDT = auto()
    BICOUSDT = auto()
    FLUXUSDT = auto()
    FXSUSDT = auto()
    VOXELUSDT = auto()
    HIGHUSDT = auto()
    CVXUSDT = auto()
    PEOPLEUSDT = auto()
    OOKIUSDT = auto()
    SPELLUSDT = auto()
    USTUSDT = auto()
    JOEUSDT = auto()
    ACHUSDT = auto()
    IMXUSDT = auto()
    GLMRUSDT = auto()
    LOKAUSDT = auto()
    SCRTUSDT = auto()
    API3USDT = auto()
    BTTCUSDT = auto()
    ACAUSDT = auto()
    ANCUSDT = auto()
    XNOUSDT = auto()
    WOOUSDT = auto()
    ALPINEUSDT = auto()
    TUSDT = auto()
    ASTRUSDT = auto()
    NBTUSDT = auto()
    GMTUSDT = auto()
    KDAUSDT = auto()
    APEUSDT = auto()
    BSWUSDT = auto()
    BIFIUSDT = auto()
    MULTIUSDT = auto()
    STEEMUSDT = auto()
    MOBUSDT = auto()
    NEXOUSDT = auto()
    REIUSDT = auto()
    GALUSDT = auto()
    LDOUSDT = auto()
