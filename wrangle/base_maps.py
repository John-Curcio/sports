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
}

# MANUAL_ESPN_BFO_MAP = {
#     # 3041602/brianna-fortino
#     '3041602': '/fighters/Brianna-Fortino-13884',
#     # /3153355/uyran-carlos
#     '3153355': '/fighters/Uyran-Carlos-11754',
#     # 3146349/carlos-leal
#     '3146349': '/fighters/Carlos-Leal-Miranda-7744',
#     # 3153355/uyran-carlos
#     '3153355': '/fighters/Uyran-Carlos-11754',
#     # 4916590/diego-dias
#     '4916590': '/fighters/Diego-Dias-11750',
#     # 2431314/jacare-souza
#     '2431314': '/fighters/Ronaldo-Souza-725',
#     # 2555633/jj-ambrose
# #     '2555633': '/fighters/J-J-Ambrose-459',
#     # /2558487/tony-johnson-jr
#     '2558487': '/fighters/Tony-Johnson-918',
#     # 2504175/zachary-micklewright
#     '2504175': '/fighters/Zach-Micklewright-1651',
#     # rodrigo de lima
#     '3110330': '/fighters/Rodrigo-Goiana-de-Lima-4992',
#     # /4030644/marcelo-rojo
#     '4030644': '/fighters/Marcelo-Rojo-7706',
#     # 3083639/mike-erosa
#     '3083639': '/fighters/Mikey-Erosa-7707',
# #     '4335927/levy-saul-marroquin-salazar'
#     '4335927': '/fighters/Levy-Saul-Marroquin-7713',
#     '4063869': '/fighters/John-Castaneda-7396',
    
#     '4423264': 'fighters/Tofiq-Musaev-9177',
#     '4306125': '/fighters/Gabe-Green-10506',
#     '4914568': '/fighters/Pete-Rodrigue-13104',
#     '3091146': '/fighters/Toninho-Gavinho-11224',
#     '3074493': '/fighters/Alexandra-Albu-7261',
#     '2509773': '/fighters/Shintaro-Ishiwatari-7509',
#     '2500906': '/fighters/Bozigit-Ataev-9050',
#     '4405109': '/fighters/Su-Mudaerji-9345',
# }