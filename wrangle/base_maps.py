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
MANUAL_ESPN_DROP_PAIR = ("2561001/bruno-carvalho", "2488370/eiji-mitsuoka")

MANUAL_ESPN_OVERWRITE_MAP = {
    # These espn IDs correspond to the same guy - they have to be merged
    # "2583704": "2613376",
    "2583704/luis-ramos": "2613376/luis-ramos",
}

MANUAL_BFO_OVERWRITE_MAP = {
    # certain bestfightodds.com fighter histories are split btw multiple pages
    '/fighters/Shintaro-Ishiwatar-1151': '/fighters/Shintaro-Ishiwatari-7509',
    '/fighters/Paddy-Holohan-2786': '/fighters/Patrick-Holohan-4991',
    '/fighters/Robert-McDaniel-4064': '/fighters/Bubba-McDaniel-744',
    '/fighters/Nicholas-Musoke-4199': '/fighters/Nico-Musoke-2144',
    '/fighters/Marco-Polo-Reyes-6679': '/fighters/Polo-Reyes-5991',
    '/fighters/Pingyuan-Liu-7732': '/fighters/Liu-Pingyuan-8739',
    '/fighters/Luis-Luna-7785': '/fighters/Anselmo-Luis-Luna-Jr-4330',
    '/fighters/Jung-Bu-Kyung-670': '/fighters/Bukyung-Jung-445',
    '/fighters/Brianna-van-Buren-4076': '/fighters/Brianna-Fortino-13884',
    '/fighters/J-J-Ambrose-12683': '/fighters/J-J-Ambrose-459',
    '/fighters/Anthony-Waldburger-1564': '/fighters/T-J-Waldburger-2156',
    '/fighters/Jadamba-Narantungalag-2028': '/fighters/Narantungalag-Jadambaa-6335', 
#     '/fighters/Narantungalag-Jadambaa-6335': '/fighters/Jadamba-Narantungalag-2028',
    '/fighters/Raquel-Paaluhi-2813': '/fighters/Raquel-Pa-aluhi-5257',
    '/fighters/Rodrigo-Cavalheiro-Correia-5516': '/fighters/Rodrigo-Cavalheiro-4743',
    '/fighters/Jesse-Miele-5797': '/fighters/Jessy-Miele-8855',
    '/fighters/Jp-Buys-12275': '/fighters/J-P-Buys-7455',
    '/fighters/Levy-Marroquin-9617': '/fighters/Levy-Saul-Marroquin-7713',
    '/fighters/Guilherme-Faria-8090': '/fighters/Guillerme-Faria-12163',
    '/fighters/Gabriel-Green-6587': '/fighters/Gabe-Green-10506',
    '/fighters/Philip-Rowe-9379': '/fighters/Phil-Rowe-9898',
    '/fighters/Phillip-Rowe-11319': '/fighters/Phil-Rowe-9898',
    '/fighters/Aleksandra-Albu-5539': '/fighters/Alexandra-Albu-7261',
    '/fighters/Bazigit-Ataev-8579': '/fighters/Bozigit-Ataev-9050',
    '/fighters/Khalil-Rountree-Jr-11552': '/fighters/Khalil-Rountree-4935',
    '/fighters/Khalil-Rountree-Jr-13118': '/fighters/Khalil-Rountree-4935',
    '/fighters/Sumudaerji-Sumudaerji-8746': '/fighters/Su-Mudaerji-9345',
}

MANUAL_UFC_ESPN_MAP = {
    # david abbott
    "http://ufcstats.com/fighter-details/b361180739bed4b0": "2354050/david-abbott",
    # dave beneteau
    "http://ufcstats.com/fighter-details/9fd1f08dd4aec14a": "2951146/dave-beneteau",
    # chris brennan
    "http://ufcstats.com/fighter-details/b19fc66613dc75b9": "2500426/chris-brennan",
    # royce gracie
    "http://ufcstats.com/fighter-details/429e7d3725852ce9": "2335697/royce-gracie",
    # dan severn
    "http://ufcstats.com/fighter-details/c670aa48827d6be6": "2335726/dan-severn",
    # keith hackney
    "http://ufcstats.com/fighter-details/bf12aca029bfcc47": "2559190/keith-hackney",
    # steve jennum
    "http://ufcstats.com/fighter-details/ad047e3073a775f3": "2951265/steve-jennum",
    # todd medina
    "http://ufcstats.com/fighter-details/b60391da771deefe": "2488903/todd-medina",
    # gary goodridge
    "http://ufcstats.com/fighter-details/fbbde91f7bc2d3c5": "2473804/gary-goodridge",
    # scott ferrozzo
    "http://ufcstats.com/fighter-details/977081bc01197656": "2499918/scott-ferrozzo",
    # joe charles
    "http://ufcstats.com/fighter-details/19ffeb5e3fffd6d5": "2499919/joe-charles",
    # anthony macias
    "http://ufcstats.com/fighter-details/dedc3bb440d09554": "2500342/anthony-macias",
    # marcus bossett
    "http://ufcstats.com/fighter-details/e8fb8e53bc2e29d6": "2951151/marcus-bossett",
    ### current top ranked in each weight class
    # deiveson figueiredo
    "http://ufcstats.com/fighter-details/aa72b0f831d0bfe5": "4189320/deiveson-figueiredo",
    # aljamain sterling
    "http://ufcstats.com/fighter-details/cb696ebfb6598724": "3031559/aljamain-sterling",
    # alex volkanovski
    "http://ufcstats.com/fighter-details/e1248941344b3288": "3949584/alexander-volkanovski",
    # islam makhachev
    "http://ufcstats.com/fighter-details/275aca31f61ba28c": "3332412/islam-makhachev",
    # leon edwards
    "http://ufcstats.com/fighter-details/f1fac969a1d70b08": "3152929/leon-edwards",
    # alex pereira
    "http://ufcstats.com/fighter-details/e5549c82bfb5582d": "4705658/alex-pereira",
    # jiri prochazka
    "http://ufcstats.com/fighter-details/009341ed974bad72": "3156612/jiri-prochazka",
    # francis ngannou
    "http://ufcstats.com/fighter-details/8d03ce87ca14e778": "3933168/francis-ngannou",
    # amanda nunes
    "http://ufcstats.com/fighter-details/80fa8218c99f9c58": "2516131/amanda-nunes",
    # zhang weili
    "http://ufcstats.com/fighter-details/1ebe20ebbfa15e29": "4350762/zhang-weili",
    # valentina shevchenko
    "http://ufcstats.com/fighter-details/132deb59abae64b1": "2554705/valentina-shevchenko",
}

MANUAL_BFO_ESPN_MAP = {
    ### current top ranked in each weight class
    # deiveson figueiredo
    "/fighters/Deiveson-Figueiredo-7514": "4189320/deiveson-figueiredo",
    "/fighters/Deiveson-Alcantra-Figueiredo-7063": "4189320/deiveson-figueiredo",
    # aljamain sterling
    "/fighters/Aljamain-Sterling-4688": "3031559/aljamain-sterling",    
    # alex volkanovski
    "/fighters/Alex-Volkanovski-6723": "3949584/alexander-volkanovski",
    "/fighters/Alexander-Volkanovski-9523": "3949584/alexander-volkanovski",
    # islam makhachev
    "/fighters/Islam-Makhachev-5541": "3332412/islam-makhachev",
    # leon edwards
    "/fighters/Leon-Edwards-4608": "3152929/leon-edwards",
    # alex pereira
    "/fighters/Alex-Pereira-10463": "4705658/alex-pereira",
    # jiri prochazka
    "/fighters/Jiri-Prochazka-6058": "3156612/jiri-prochazka",
    # francis ngannou
    "/fighters/Francis-Ngannou-5847": "3933168/francis-ngannou",
    # amanda nunes
    "/fighters/Amanda-Nunes-2225": "2516131/amanda-nunes",
    # zhang weili
    "/fighters/Zhang-Weili-9795": "4350762/zhang-weili",
    "/fighters/Weili-Zhang-7955": "4350762/zhang-weili",
    # valentina shevchenko
    "/fighters/Valentina-Shevchenko-5475": "2554705/valentina-shevchenko",
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