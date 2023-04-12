import pandas as pd

NAME_REPLACE_DICT = {
    "julianna peña": "julianna pena",
    "marco polo reyes": "polo reyes",
    "brad scott": "bradley scott",
    "nicholas musoke": "nico musoke",
    "paddy holohan": "patrick holohan",
    "alatengheili": "alateng heili",
    "ode osbourne": "ode' osbourne",
    "zhang tiequan": "tiequan zhang",
    "aleksandra albu": "alexandra albu",
    "alvaro herrera mendoza": "alvaro herrera",
    "sumudaerji": "su mudaerji",
    "mark madsen": "mark o. madsen",
    "pingyuan liu": "liu pingyuan",
    "robert mcdaniel": "bubba mcdaniel",
    "aoriqileng": "aori qileng",
    "robert sanchez": "roberto sanchez",
    "patrick smith": "patrick trey smith",
    "aleksandra albu": "alexandra albu",
    "jiří procházka": "jiri prochazka",
}

# 2561001/bruno-carvalho didn't fight 2488370/eiji-mitsuoka on Jul 16, 2011
# that was a different bruno carvalho, who is already in the dataset
# MANUAL_ESPN_DROP_PAIR = ("2561001", "2488370")
MANUAL_ESPN_DROP_FIGHTS = pd.DataFrame([
    # bruno carvalho vs eiji mitsuoka on Jul 16, 2011
    # the right bruno carvalho is "3104455"
    ("Jul 16, 2011", "2561001", "2488370"),
    # luis ramos vs erick silva on Aug 27, 2011
    # the right luis ramos is "2613376"
    ("Aug 27, 2011", "2583704", "2559966"),
    # larue burley vs bubba jenkins on sept 20, 2013
    # the right bubba jenkins is "3146832"
    ("Sep 20, 2013", "3058471", "2611321"),
    
], columns=["Date", "FighterID", "OpponentID"])
MANUAL_ESPN_DROP_FIGHTS["Date"] = pd.to_datetime(MANUAL_ESPN_DROP_FIGHTS["Date"])

MANUAL_UFC_DROP_FIGHTS = [ 
    # kazushi sakuraba fought marcus silveira twice on dec 21, 1997
    # here, I drop the first fight, which was overturned to a no contest
    "http://ufcstats.com/fight-details/2750ac5854e8b28b",
]

MANUAL_ESPN_OVERWRITE_MAP = {
    # These espn IDs correspond to the same guy - they have to be merged
#     "2583704": "2613376",

#     # "2583704/luis-ramos": "2613376/luis-ramos",
    # these two alex shoenauer IDs correspond to the same guy
    # https://www.sherdog.com/fighter/Alex-Schoenauer-4000
    # https://www.tapology.com/fightcenter/fighters/alex-schoenauer
    "2951428": "2335745",
}

MANUAL_BFO_OVERWRITE_MAP = {
    # certain bestfightodds.com fighter histories are split btw multiple pages
    'Shintaro-Ishiwatar-1151': 'Shintaro-Ishiwatari-7509',
    'Paddy-Holohan-2786': 'Patrick-Holohan-4991',
    'Robert-McDaniel-4064': 'Bubba-McDaniel-744',
    'Nicholas-Musoke-4199': 'Nico-Musoke-2144',
    'Marco-Polo-Reyes-6679': 'Polo-Reyes-5991',
    'Pingyuan-Liu-7732': 'Liu-Pingyuan-8739',
    'Luis-Luna-7785': 'Anselmo-Luis-Luna-Jr-4330',
    'Jung-Bu-Kyung-670': 'Bukyung-Jung-445',
    'Brianna-van-Buren-4076': 'Brianna-Fortino-13884',
    'J-J-Ambrose-12683': 'J-J-Ambrose-459',
    'Anthony-Waldburger-1564': 'T-J-Waldburger-2156',
    'Jadamba-Narantungalag-2028': 'Narantungalag-Jadambaa-6335', 
#     'Narantungalag-Jadambaa-6335': 'Jadamba-Narantungalag-2028',
    'Raquel-Paaluhi-2813': 'Raquel-Pa-aluhi-5257',
    'Rodrigo-Cavalheiro-Correia-5516': 'Rodrigo-Cavalheiro-4743',
    'Jesse-Miele-5797': 'Jessy-Miele-8855',
    'Jp-Buys-12275': 'J-P-Buys-7455',
    'Levy-Marroquin-9617': 'Levy-Saul-Marroquin-7713',
    'Guilherme-Faria-8090': 'Guillerme-Faria-12163',
    'Gabriel-Green-6587': 'Gabe-Green-10506',
    'Philip-Rowe-9379': 'Phil-Rowe-9898',
    'Phillip-Rowe-11319': 'Phil-Rowe-9898',
    'Aleksandra-Albu-5539': 'Alexandra-Albu-7261',
    'Bazigit-Ataev-8579': 'Bozigit-Ataev-9050',
    'Khalil-Rountree-Jr-11552': 'Khalil-Rountree-4935',
    'Khalil-Rountree-Jr-13118': 'Khalil-Rountree-4935',
    'Sumudaerji-Sumudaerji-8746': 'Su-Mudaerji-9345',

    "Cheyanne-Buys-10252": "Cheyanne-Vlismas-8146",

    "Joanne-Calderwood-3554": "Joanne-Wood-12845",

    "Nuerdanbieke-Shayilan-12818": "Shayilan-Nuerdanbieke-11334",
    "Michael-Malott-13500": "Mike-Malott-13089",
    "Aori-Qileng-13588": "Qileng-Aori-10837",
    "Brogan-Walker-Sanchez-8337": "Brogan-Walker-7832",
}

MANUAL_UFC_ESPN_MAP = {
    # david abbott
    "b361180739bed4b0": "2354050",
    # dave beneteau
    "9fd1f08dd4aec14a": "2951146",
    # chris brennan
    "b19fc66613dc75b9": "2500426",
    # royce gracie
    "429e7d3725852ce9": "2335697",
    # dan severn
    "c670aa48827d6be6": "2335726",
    # keith hackney
    "bf12aca029bfcc47": "2559190",
    # steve jennum
    "ad047e3073a775f3": "2951265",
    # todd medina
    "b60391da771deefe": "2488903",
    # gary goodridge
    "fbbde91f7bc2d3c5": "2473804",
    # scott ferrozzo
    "977081bc01197656": "2499918",
    # joe charles
    "19ffeb5e3fffd6d5": "2499919",
    # anthony macias
    "dedc3bb440d09554": "2500342",
    # marcus bossett
    "e8fb8e53bc2e29d6": "2951151",
    ### current top ranked in each weight class
    # deiveson figueiredo
    "aa72b0f831d0bfe5": "4189320",
    # aljamain sterling
    "cb696ebfb6598724": "3031559",
    # alex volkanovski
    "e1248941344b3288": "3949584",
    # islam makhachev
    "275aca31f61ba28c": "3332412",
    # leon edwards
    "f1fac969a1d70b08": "3152929",
    # alex pereira
    "e5549c82bfb5582d": "4705658",
    # jiri prochazka
    "009341ed974bad72": "3156612",
    # francis ngannou
    "8d03ce87ca14e778": "3933168",
    # amanda nunes
    "80fa8218c99f9c58": "2516131",
    # zhang weili
    "1ebe20ebbfa15e29": "4350762",
    # valentina shevchenko
    "132deb59abae64b1": "2554705",
}

FALSE_OVERWRITE_UFC_MAP = {
    # some pages in bestfightodds.com actually comprise data for multiple fighters,
    # who happen to have the same name. EG 
    # joey gomez:
    # https://www.bestfightodds.com/fighters/Joey-Gomez-6023 
    # http://ufcstats.com/fighter-details/0778f94eb5d588a5
    # http://ufcstats.com/fighter-details/3a28e1e641366308
    # So when we're trying to join BFO and UFC data, we temporarily merge the
    # UFC IDs for these fighters into one, and then split them again later.
    
    # joey gomez: Joey-Gomez-6023
    "3a28e1e641366308": "0778f94eb5d588a5",
    # bruno silva: Bruno-Silva-7121
    "12ebd7d157e91701": "294aa73dbf37d281",
}

FALSE_OVERWRITE_ESPN_MAP = {
    # some pages in bestfightodds.com actually comprise data for multiple fighters,
    # who happen to have the same name. 
    # So when we're trying to join BFO and ESPN data, we temporarily merge the
    # ESPN IDs for these fighters into one, and then split them again later.
# https://www.bestfightodds.com/fighters/Alex-Schoenauer-335
    '2951428': '2335745',
# https://www.bestfightodds.com/fighters/Bruno-Carvalho-2451
    '3104455': '2561001',
# https://www.bestfightodds.com/fighters/Bruno-Silva-7121
    '4333158': '3895544',
# https://www.bestfightodds.com/fighters/Elias-Garcia-4042
    '3111338': '3028421',
# https://www.bestfightodds.com/fighters/Elijah-Harris-9867
    '4455612': '5119542',
# https://www.bestfightodds.com/fighters/Joey-Gomez-6023
    '4357555': '3947131',
# https://www.bestfightodds.com/fighters/Jordan-Johnson-3341
    '3082396': '4070161',
# https://www.bestfightodds.com/fighters/Magomed-Idrisov-7339
    '5126239': '4011477',
# https://www.bestfightodds.com/fighters/Michael-Graves-5242
    '4342232': '3890654',
# https://www.bestfightodds.com/fighters/Rafael-Dias-527
    '4325203': '2554931',
# https://www.bestfightodds.com/fighters/Rafael-Silva-4257
    '2511421': '2957832',
# https://www.bestfightodds.com/fighters/Steve-Garcia-2576
    '4237933': '3023804',
# https://www.bestfightodds.com/fighters/Thiago-Santos-2526
    '2559760': '3045798',
    '4710380': '3045798',
}

MANUAL_BFO_UFC_MAP = {
    ### current top ranked in each weight class
    # deiveson figueiredo
    "Deiveson-Figueiredo-7514": "aa72b0f831d0bfe5",
    "Deiveson-Alcantra-Figueiredo-7063": "aa72b0f831d0bfe5",
    # aljamain sterling
    "Aljamain-Sterling-4688": "cb696ebfb6598724",
    # alex volkanovski
    # http://ufcstats.com/fighter-details/e1248941344b3288
    "Alex-Volkanovski-6723": "e1248941344b3288",
    "Alexander-Volkanovski-9523": "e1248941344b3288",
    # islam makhachev http://ufcstats.com/fighter-details/275aca31f61ba28c
    "Islam-Makhachev-5541": "275aca31f61ba28c",
    # leon edwards http://ufcstats.com/fighter-details/f1fac969a1d70b08
    "Leon-Edwards-4608": "f1fac969a1d70b08",
    # alex pereira http://ufcstats.com/fighter-details/e5549c82bfb5582d
    "Alex-Pereira-10463": "e5549c82bfb5582d",
    # jiri prochazka http://ufcstats.com/fighter-details/009341ed974bad72
    "Jiri-Prochazka-6058": "009341ed974bad72",
    # francis ngannou http://ufcstats.com/fighter-details/8d03ce87ca14e778
    "Francis-Ngannou-5847": "8d03ce87ca14e778",
    # amanda nunes http://ufcstats.com/fighter-details/80fa8218c99f9c58
    "Amanda-Nunes-2225": "80fa8218c99f9c58",
    # zhang weili http://ufcstats.com/fighter-details/1ebe20ebbfa15e29
    "Zhang-Weili-9795": "1ebe20ebbfa15e29",
    "Weili-Zhang-7955": "1ebe20ebbfa15e29",
    # valentina shevchenko http://ufcstats.com/fighter-details/132deb59abae64b1
    "Valentina-Shevchenko-5475": "132deb59abae64b1",
    ### manually resolving conflicts
    # james terry was supposed to fight brett bergmark, but bergmark was pulled
    # so terry ended up fighting david marshall
    "James-Terry-1155": "5befa793316d3469",
    "Brett-Bergmark-1973": "2891c9f7b26b26fc",
    # loma lookboonmee was supposed to fight diana belbirr/belbita, but belbita was pulled
    # so lookboonmee ended up fighting denise gomes
    "Loma-Lookboonmee-7834": "4e30e4250cb08b61",
    "Diana-Belbirr-13003": "d7e40dca3ae125be",
    "Denise-Gomes-13433": "cffb87059e645bd1",
    # gerald meerschaert was supposed to fight abusupiyan magomedov on Dec 18, 2021
    # but meerschaert ended up fighting dustin stoltzfus
    "Gerald-Meerschaert-3628": "6ac9bc2953c47345",
    "Abusupiyan-Magomedov-8265": "36b8f265bcd1b7a4",
    "Dustin-Stoltzfus-10214": "71505842fb6455c3",
    # mallory martin was supposed to fight either montserrat ruiz,
    # cheyanne buys, or cheyanne vlismas on Dec 4, 2021. She ended up
    # fighting cheyanne vlismas (same person as cheyanne buys)
    # montserrat ruiz/conejo 
    "Montserrat-Ruiz-8705": "1235b31de15d0c6e",
    "Cheyanne-Vlismas-8146": "4959f10a62f3fec3",

    # joanne wood 
    "Joanne-Wood-12845": "12f91bfa8f1f723b",

    # ryan barberena was supposed to fight matt brown on Dec 4, 2021
    # but barberena ended up fighting darian weeks
    "Bryan-Barberena-4982": "a331233f597090a5",
    "Matt-Brown-6270": "31123249b0bbf52e",
    "Darian-Weeks-10594": "326a001926ffb7ec",
    # josiane nunes was was supposed to fight jennifer gonzalez on feb 26, 2022
    # but nunes ended up fighting ramona pascual
    "Josiane-Nunes-11029": "68d35296f566792b",
    "Ramona-Pascual-13314": "3eb4dc9a7d4ac906",
    # francisco figueiredo was supposed to fight daniel lacerda on apr 30, 2022
    # but figueiredo ended up fighting daniel da silva
    "Francisco-Figueiredo-12945": "cdadae5363b66eef",
    "Daniel-da-Silva-13133": "31bb0772f21cabd8",
    # orion cosce was supposed to fight mike mathetha on jul 20, 2022
    # but ended up fighting blood diamond
    "Orion-Cosce-10253": "2c010b26a3306969",
    "Blood-Diamond-13276": "9edf2c9082cc2cd8",
    
}

MANUAL_BFO_ESPN_MAP = {
    ### current top ranked in each weight class
    # deiveson figueiredo
    "Deiveson-Figueiredo-7514": "4189320",
    "Deiveson-Alcantra-Figueiredo-7063": "4189320",
    # aljamain sterling
    "Aljamain-Sterling-4688": "3031559",
    # alex volkanovski
    "Alex-Volkanovski-6723": "3949584",
    "Alexander-Volkanovski-9523": "3949584",
    # islam makhachev
    "Islam-Makhachev-5541": "3332412",
    # leon edwards
    "Leon-Edwards-4608": "3152929",
    # alex pereira
    "Alex-Pereira-10463": "4705658",
    # jiri prochazka
    "Jiri-Prochazka-6058": "3156612",
    # francis ngannou
    "Francis-Ngannou-5847": "3933168",
    # amanda nunes
    "Amanda-Nunes-2225": "2516131",
    # zhang weili
    "Zhang-Weili-9795": "4350762",
    "Weili-Zhang-7955": "4350762",
    # valentina shevchenko
    "Valentina-Shevchenko-5475": "2554705",
    ####### resolving conflicts ########
    "Thiago-Santos-2526": "3045798",

    "Brett-Bergmark-1973": "2504260",
    
    "Carlos-Leal-Miranda-7744": "3146349", # (most similar to "carlos leal")
    "Rodolfo-Marques-2711": "2578777", # (most similar to "rodolfo marques diniz")
    "Teodoras-Aukstulis-6120": "3955016", # (most similar to "teodoras aukstuolis")
    "Kevin-Morerya-7709": "4393564", # (most similar to "kevin moreyra")
    "Mikey-Erosa-7707": "3083639", # (most similar to "mike erosa")
    "Sunna-Rannveig-Davidsdottir-9086": "4048393", # (most similar to "sunna davidsdottir")
    "Diana-Belbirr-13003": "3977987", # (most similar to "diana belbita")

    "Diego-Dias-11750": "4916590", # (most similar to "diego dias")

    "Robson-Junior-11403": "4917065",

    "Lom-Ali-Eskijew-8135": "4321977",

    "Zarah-Fairn-Dos-Santos-7522": "4078218",
    "Abusupiyan-Magomedov-8265": "3077822",
    "Montserrat-Ruiz-8705": "4399703",
    
    # mappings courtesy of ChatGPT
    "Carlos-Graca-8951": "5075977",
    "Matej-Penaz-10083": "5060472",
    "Roybert-Echeverria-11653": "5075937",
    "Earnest-Walls-11682": "4565643",
    "Kory-Moegenburg-12251": "4231045",
    "Bobby-Seronio-III-12458": "4884789",
    "Socrates-Hernandez-10003": "4884791",
    "Felipe-Maia-12686": "4808689",
    "Tamika-Jones-13091": "5121336",
    # "Carlos-Leal-Miranda-7744": "3146349",
    "Kyra-Batara-4933": "3796887",
    "Cleveland-McLean-9934": "4036098",
    "Jacob-Silva-5302": "4193828",
    "Daniel-Lacerda-12102": "4895772",
    "Szymon-Bajor-5517": "3093739",
    "Ronny-Markes-2605": "2559755",
    "Topnoi-Kiwram-13938": "4711335",
    "Tyler-Flores-10228": "4010873",
    "Paulo-Laia-15272": "4870267",
    "Kaytlin-Neil-7549": "4244337",
}

MANUAL_THE_ODDS_ESPN_MAP = {
    "Austin Lingo": "4570669",
    "Da Un Jung": "4389252",
    "Leon Edwards": "3152929",
    "Weili Zhang": "4350762",
    "Marlon Vera": "3155424",
    "Holly Holm": "3028404",
    "Patrick Downey": "4913863",
    "Juliana Miller": "4912582",

    "Rick Glenn": "3030256",
    "Sergey Pavlovich": "4217395",
    "Shamil Abdurahimov": "2558062",
    "Abusupyian Magomedov": "3077822",
    "Carlos Diego Ferreira": "3026133",
    "Maxim Grishin": "2558492",

    "Zarah Fairn Dos Santos": "4078218",
    "Orion Cosce": "4687126",

    "Deiveson Figueiredo": "4189320",
    "Amanda Nunes": "2516131",
}