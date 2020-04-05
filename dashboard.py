#!/usrbin/env python3

import numpy as np
import pandas as pd
import re

from collections import Counter
from dateutil import parser

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Plot.ly dashboard interface
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

NEED = 'deduped_need'

split_re = re.compile('(,|;| and| &) ')

# First level de-duplication map
# TODO there's one issue here, I haven't done proper sentence tokenization,
# so sometimes have multiple requests. I've put each in the best category I can for now.
REVERSE_MAP = {
    'N95 mask': [
        'N95s', '(N95s', '8511 Masks', 'N95 masks x 4', '3M N95 masks', 'N99 Masks', 'Health grade N95 Masks',
        'N95 fit and face masks.', '#1 need N95 nurses almost out', 'N95 masks 1870', 'N100', 'P100,R100', 'N99',
        'P99', 'R99', 'P95 and R95', 'N95 Niosh', 'P100', 'N95', 'FFP3', 'FFP2', 'but prefer N95 or surgical masks.',
        'N95 fit', 'facemasks >N95 protection level (e.g. N99 or above)',
        'N95s for a medical office of 11 staff members',
        "NEED N95's Yesterday!!!! Please get them here ASAP",
        'NIOSH approved N95 masks', ],
    'Surgical/Procedure/Cone masks': [
        'Surgical Masks', 'surgical/procedural masks', 'Surgical masks', 'Cone Masks', 'Any masks', 'surgical masks'
                                                                                                    'respirator and isolation masks',
        'Level 1 masks', 'Standard Precaution Mask', 'Disposable face masks',
        'level 1 face masks with ear loops', 'Level 2 face masks', 'M3 reusable masks', 'Masks', 'face masks.',
        'Reusable cloth masks', 'We can use tie in back surgical/ n95 mask covers.', 'isolation masks', ],
    'Size Small N95 Masks': [
        'We need SIZE SMALL N95s', 'Small N95 Mask', 'size small nitrile gloves'],
    None: [
        'Needs confirmation (potentially accepting homemade masks)'],
    'Safety Goggles/Glasses': [
        'Safety Goggles', 'Safety goggles', 'goggles (non-vented)', 'EMS Safety Glasses'],
    'Face Shields': ['Face Shields', 'Face shields', 'Face Ahields', 'splash shields',
                     'very much in need of face shields!',
                     'non-disposable face shields for use with chemicals/machining'],
    'Safety Goggles/Face Shields': [
        'goggles/eye protection', 'Eye protection', 'EYE PROTECTION', 'eye protection', 'Protective Eye Wear',
        'Eye Protection (reusable or single use eye shields)', 'eye guards'],
    'Mask w/ Face Shield': [
        'face shield masks', 'Mask with face shield', 'procedure mask with visor'],
    'Disposable Booties': ['Disposable Booties', 'Disposable Gooties', 'Disposable booties', 'shoe covers',
                           'shoe covers (need sizes medium-large)'],
    'Gloves': [
        'Gloves', 'nitrile gloves', 'Gloves (Vinyl or Nitrile)', 'Nitrile Gloves',
        'Long gloves (nitrile or other)', 'Chemical impervious gloves', 'gloves (SM/MD/LG)'],
    'Isopropyl/Rubbing Alcohol': [
        'Isopropyl Alcohol', 'rubbing alcohol', 'Rubbing Alcohol', 'Alcohol', 'alcohol',
        'Isopropyl alcohol', 'Isopropyl Alcohol bottles or prep pads'],
    'Isopropyl Prep Pads': ['alcohol wipes', 'Alcohol Prep Pads', 'Alcohol wipes', 'Alcohol prep pads'],
    'Surgical/Other Head/Hair coverings': [
        'Head caps', 'Disposable surgical caps', 'head covers', 'Bouffant style surgical caps',
        'Head Wear', 'hair nets', 'caps', 'Hairnets', 'bonnets', 'Disposable hair protection',
        'disposable bouffant type with elastic band', 'Bouffant caps', 'Head covers'],
    'Disposable/Surgical Gowns': [
        'Gowns', 'disposable gowns', 'GOWNS ARE TOP PRIORITY', 'isolation gown', 'yellow gowns',
        'Disposable gowns', 'Paper Gowns', 'gown', 'isolation or surgical gowns', 'Gown', 'Isolation gowns',
        'gowns', 'Lab coats or gowns', 'Isolation or Surgical Gowns', 'gowns. all sizes.', 'medical long sleeve gowns',
        'PPE Uniforms', 'isolation gowns/hoods', 'Disposable Gowns', 'Permeable gowns', 'chemo gowns',
        'Gowns are in extreme needs.', 'Gowns - Anything will do'],
    'Gowns, Scrubs, and Bunny Suits': [
        'Gowns/bunny suits', 'hazmat suits', 'Ponchos', 'disposable rain ponchos', 'Disposable rain coats',
        'full coverage coverall / head cover'],
    'Bunny Suits': [
        'Bunny suits', 'Coveralls', 'coveralls', 'Bunny Suits', 'bunny suits', 'Tyvek Paint Suits',
        'Impermeable painter smock', 'Tyvek Suits', 'Hazmat bunny suits', 'Coveralls/bunny suits'],
    'scrubs': [
        'scrubs', 'Scrubs', 'Surgical Scrubs', 'Any new scrubs or lab coats (including homemade)',
        'protective plastic scrubs'],
    'Disinfecting Wipes (general)': [
        'Disinfectant wipes', 'Disinfectant Wipes', 'disinfectant wipes', 'disinfecting wipes', 'Sanitizing Wipes',
        'Disinfecting wipes', 'any Clorox or sani-cloth wipes', 'Cleaning wipes hospital grade',
        'hydrogen peroxide wipes', 'Sani-Wipes', 'sani cloths'],
    'Disinfecting Wipes (bleach)': [
        'bleach wipes', 'Clorox wipes', 'Bleach Wipes'],
    'Disinfecting Wipes (no bleach)': ['non-bleach wipes', 'surgical or NOT Lysol or Clorox wipes'],
    'Lysol/Bleach': ['Lysol Spray', 'Lysol', 'Bleach', 'bleach'],
    'Other Disinfectants': [
        'antimicrobial cleaners (Cavicide liquid or wipes)', 'Desperately need floor Disinfectant',
        'Cleaning supplies', 'germicides', 'Citrace', 'Cleaning Supplies', 'Peroxide',
        'Distilled Water for Autoclave Machine & Disinfecting Spray', 'unopened sanitizer or disinfectant products',
        'sanitizing gels and sprays', 'Disinfecting Spray'],
    'Hand Sanitizer': [
        'Hand Sanitizer', 'hand sanitizer)', 'Hand sanitizer', 'Liquid hand sanitizer', 'Liquid Hand Sanitizer',
        'Hand sanitizers', 'sanitizer', 'hand sanitizer', 'Hand sanitizer,', 'Alcohol based Hand Sanitizer',
        'sanitizing gels'],
    'NON-alcohol hand cleaners': [
        'General purpose hand cleaners', 'Hands Soap', 'Non alcohol hand sanitizer', 'Antibacterial soap'],
    'Powered Air Purifying Respirator (PAPR)': [
        'Positive Airway Purifying Respirator (PARP)', 'Powered\\, air-purifying respirator (PAPR)', "PAPR's",
        'PAPRs', 'PAPR', 'PAPPR', 'PAPR hoods (personal air purifiers) (with batteries, motors, filters)',
        'PAPRS and PAPR hoods', 'PAPRS', 'Papers', 'PAPR equipment', 'Papr', 'PAPRS badly'],
    'Controlled Air Purifying Respirator (CAPR)': ['CAPRS', 'CAPR', 'MAXAIR CAPR', 'CAPRs'],
    'PAPR/CAPR': [
        'CAPR/PAPR machines', 'PAPR/CAPR Respirators', 'CAPR and PAPR equipment', 'air-purifying respirator (PAPR)'],
    'Reusable Respirators': [
        'Respirators', 'Respirator equipment', 'respirators', 'Full face respirators with HEPA Filters',
        'N95 respirators PAPRs', 'Respirators (Medical or industrial)', 'Reusable Respirators', 'respirtaors',
        'respirator', ],
    'Air Purifying Respirator Consumables': [
        'CAPR/PAPR machines and disposables', 'respirator filters',
        'Disposable/Replacement Parts (HoodsBattery PackTubing)'],
    'C/PAPR Hood': [
        'CAPR face shields', 'PAPR hoods', 'Maxair CAPR shields', 'Powered air purifying respirator (PAPR) hoods',
        'PAPR masks', 'Hoods.', 'CAPR shields', 'PAPR hoods (personal air purifiers) (with batteries', ],
    'Respirator Consumables': ['N100 cartridges', '3M respirator n95 or p100 replacement filters',
                               "Other respirators (P100's\\, and PAPR supplies / parts)",
                               'Disposable/Replacement Parts (Hoods', 'CAPR and/ or CAPR face shields'],
    'Disposable Stethoscopes': [
        'disposable stethoscopes', 'Disposable Stethoscopes', 'Disposable stethascopes', 'disposable stethescopes',
        'Disposable stethoscopes', 'disposable stethoscope'],
    'Thermometers': ['Thermometers', 'thermometers'],
    'Infrared Thermometers': [
        'infrared thermometers', 'Infrared thermometer', 'No-Touch Thermometers',
        'Thermometers without the need of probe covers', 'Thermal scanners'],
    'Thermometer covers': ['Thermometer covers', 'thermometer caps for scanning temps', 'thermometer sheaths',
                           'probe covers for digital oral thermometers', 'Ear Thermometer covers',
                           'Thermometer probe covers', 'Thermometer Probe Covers', 'thermometer probes'],
    'Test Swabs': [
        'testing swabs', 'Swabs to test covid-19', 'swabs,', 'swabs',
        'Quidel 403C Swab Specimen Flocked Tube Flexible Minitip 50 UTM', 'flocked swabs'],
    'STD. Cotton Swabs': ['sterile cotton-tipped swabs (BD #220531)', 'Sterile cotton tipped swabs (BD #220531)'],
    'Nasopharyngeal Test Swabs': ['nasalpharyngeal swabs', '6" plastic nasal swabs', 'NP SWABS'],
    'Oral/mouth Test Swabs': ['mouth swabs'],
    'Paper products': ['Toilet Paper', 'Paper Towels', 'toilet paper', 'paper towels', 'Paper towels'],
    'Testing Kits (RNA/DNA, Flu, etc...)': [
        'testing kits', 'rapid flu testing kit', 'testing kits.',
        'Kits: QIAamp® Viral RNA Mini Kit QIAamp® MinElute Virus Spin Kit or RNeasy® Mini Kit (QIAGEN) EZ1 DSP Virus Kit (QIAGEN) Roche MagNA Pure Compact RNA Isolation Kit Roche MagNA Pure Compact Nucleic Acid Isolation Kit Roche MagNA Pure 96 DNA and Viral NA Small Volume Kit Instruments: Roche Magna Pure Roche 480Z ABI7500DX Qiagen EZ1 Advanced XL QIAcube/QIAcube Connect',
        'M4 viral media'],
    'Pulse Oximeters': [
        '2 pulse oximeters (medical grade) needed', 'Pulse oximeters', '02 monitors', 'O2 monitors'],
    'Facial Tissue': ['Kleenex'], 'Bi-/CPAP and supplies': ['BIPAP', 'CPAP masks'],
    'Acetaminophen': ['Tylenol Adults and Children', 'Acetaminophen', 'Tylenol Adults'],
    'Ventilator Consumables': ['Extension tubing for + pts on vents', 'Ventilator parts'],
    'Ventilators': ['ventilators', 'Ventilators'],
    'Other consumables': ['Needles', 'syringes', 'Biohazard bags', 'Distilled Water for Autoclave Machine',
                          'biohazard bags', 'disposable blood pressure cuffs'],
    'homemade masks--exact': ['Homemade masks (please specific what types)', ],
    'hand/home-made masks': [
        'Home sewn masks using https://www.craftpassion.com/face-mask-sewing-pattern/. We ask that you use cotton such as cotton sheets- two layers of material on the mask',
        'Home-sewn masks using https://www.craftpassion.com/face-mask-sewing-pattern/. We ask that you use cotton such as cotton sheets- two layers of material on the mask',
        'Hand-sewn face masks', 'Homemade Masks',
        'Hand sewn masks in this pattern: https://drive.google.com/file/d/1EwHrnspVWh1Z30P7VkqiIQtXhPhqmRNT/view?usp=sharing',
        'Handmade sewn masks', 'home-made masks', 'home-made Deaconess masks',
        'Sewn Masks: https://www.unitypoint.org/cedarrapids/sewing-surgical-masks.aspx',
        'Sewn Masks: Pattern https://buttoncounter.com/2018/01/14/facemask-a-picture-tutorial/',
        'Sewn Masks',
        'Handmade masks (https://chw.org/-/media/files/for-patients-and-families/covid-19/mask-guidelines.pdf?la=en)',
        'Handmade masks (https://chw.org/-/media/files/for-patients-and-families/covid-19/mask-guidelines.pdf?la=en)home-made Deaconess masks',
        'Home-made masks', 'handmade masks', 'Homemade masks', 'handmake masks', 'hand sewn masks are also acceptable',
        'fabric masks', 'Community produced PPE (ex 3D printed) please contact for details',
        'N99 Masks. Home-made masks',
        '100% cotton face masks for droplet precautions if homemade', 'Any homemade masks that can be washed',
        'Hand-Sewn Masks',
        'Handmade masks - please follow pattern (https://www.froedtert.com/sites/default/files/files/2020-03/MaskInstructions_V2.pdf)']
    }
DEDUP_MAP = {v: k for k in REVERSE_MAP for v in REVERSE_MAP[k]}
DEDUP_MAP = {k.strip(): v for k, v in DEDUP_MAP.items()}

# TODO: the authentication here is set up for user access, before deployment it will
#   need to be set up for programmatic access.

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
# TODO: this is the US data only. This will require i18n.
SAMPLE_SPREADSHEET_ID = '1GwP7Ly6iaqgcms0T80QGCNW4y2gJ7tzVND2CktFqnXM'
SAMPLE_RANGE_NAME = 'Form Responses 1!A:S'

APPROVED = 'approved'


def gyet_it():
    """
    Modified from Google Sheets example. Named, with apologies, in reference to Gravity Falls.
    Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        print('Name, Major:')
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            print('%s, %s' % (row[0], row[4]))
            break

    return values


def clean_it(data, only_approved=True):
    df = pd.DataFrame(data[1:], columns=data[0][:max(len(x) for x in data[1:])])
    cols = df.columns
    new_names = {}
    for c in cols:
        if df.loc[0, c]:
            new_names[c] = df.loc[0, c]

    df = df.rename(columns=new_names).drop(0)
    df[APPROVED] = df.approved.apply(lambda x: x == 'x')
    if only_approved:
        df = df[df[APPROVED]]
    return df


def explode_accepting(data_orig: pd.DataFrame):
    data = data_orig.copy()
    # TODO: this is where better tokenization might help
    data['accepting'] = data['accepting'].apply(lambda x: split_re.split(x) if type(x) is str else None)
    data = data.dropna(subset=['accepting'])
    dfe = data.explode('accepting')
    dfe.index = np.arange(len(dfe))  # re-index to avoid later problems with duplicated indices
    dfe['timestamp'] = dfe.timestamp.apply(parser.parse)

    dfe[NEED] = dfe.accepting.apply(lambda x: DEDUP_MAP[x] if x in DEDUP_MAP else None)
    return dfe


def log_unclassified_items(input_data):
    # Write out things that aren't in the map that maybe should be
    unclassified = Counter([x for x in input_data.accepting.values.tolist() if x not in DEDUP_MAP])
    print(unclassified.most_common(20))


def cumulative_needs(input_data: pd.DataFrame, column: str = NEED, values=None, top_n: int = 10):
    if values is None:
        values = list(input_data.groupby('deduped_need').size().sort_values(ascending=False).head(top_n).index)
    data = input_data.sort_values('timestamp').copy()
    data = data[data[NEED].isin(values)]
    cumul_data = data.copy()
    for v in values:
        cumul_data[v] = cumul_data[NEED].apply(lambda x: x == v).astype(int).cumsum()
        data[v] = data[NEED].apply(lambda x: x == v).astype(int)
    cumul_data = cumul_data[['timestamp'] + values].sort_values('timestamp').dropna()
    data = data[['timestamp'] + values].sort_values('timestamp').dropna()
    return data, cumul_data, values


def time_bin_data(input_data: pd.DataFrame, time_bin_size='6H', columns=None):
    tmp = input_data.copy()
    if columns is None:
        columns = [c for c in tmp.columns if c != 'timestamp']
    agg_funcs = {c: 'sum' for c in columns}
    out1 = tmp.groupby('timestamp').agg(agg_funcs)
    out1['new_timestamp'] = out1.index.round(time_bin_size)
    out2 = out1.groupby('new_timestamp').agg(agg_funcs).reset_index().rename(columns={'new_timestamp': 'timestamp'})
    return out2


def tidy(input_data: pd.DataFrame):
    # Hadley whats-his-face can die in a fire. I hate the tidy-verse. But to use plotly, we must do this...
    data = input_data.copy().melt(id_vars=['timestamp']).drop_duplicates()
    data = data[data['value'] != 0]
    return data


latest_data = gyet_it()
df = clean_it(latest_data)
dfe = explode_accepting(df)
needs, cum_needs, ppe_types = cumulative_needs(dfe, column='deduped_need', top_n=10)
log_unclassified_items(dfe)

app = dash.Dash(__name__)
server = app.server
ppe_types.sort()

app.layout = html.Div([
    html.Div([
        dcc.Checklist(id='cumul-select', options=[{'label': 'cumulative', 'value': 'cumulative'}],
                            value=['cumulative'], labelStyle={'display': 'inline-block'}),
        dcc.Dropdown(id='time-bin-select',
                     options=[{'label': k, 'value': k} for k in ['1H', '3H', '6H', '12H', '24H']],
                     value='6H', style={'width': '200px'}),
        html.I(" Time binning is for non-cumulative plots only")
        ]),
    html.Div([dcc.Checklist(id='ppe-type-select',
                            options=[{'label': i, 'value': i} for i in ppe_types],
                            value=ppe_types, labelStyle={'display': 'inline-block'})]),
    # TODO: use dcc.Loading to add a spinner while the graph loads.
    dcc.Graph('top10-ppe-graph', config={'displayModeBar': False})
    ])


@app.callback(
        Output('top10-ppe-graph', 'figure'),
        [Input('ppe-type-select', 'value'), Input('cumul-select', 'value'), Input('time-bin-select', 'value')]
        )
def update_needs_graph(var_selections, cumulative, time_bin):
    import plotly.express as px
    try:
        if cumulative == ['cumulative', ]:
            display_data = cum_needs.copy()
        else:
            display_data = time_bin_data(needs, time_bin_size=time_bin).copy()
        display_data = tidy(display_data)
        return px.line(display_data[display_data["variable"].isin(var_selections)],
                       x='timestamp', y='value', line_group='variable', color='variable')
    except KeyError as e:
        print(f"Available keys are: {cum_needs.columns}")
        raise e


if __name__ == '__main__':
    app.run_server(debug=False)
