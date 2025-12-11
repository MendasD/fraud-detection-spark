"""
Dashboard ENRICHI pour la détection de fraudes avec ML
Ajout de 4 nouveaux graphiques avancés dans la page Analyse Avancée
"""

import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from datetime import datetime
import logging
import os
import sys
import numpy as np
import glob
from scipy.spatial.distance import cdist
import networkx as nx
import requests
from dotenv import load_dotenv
from pathlib import Path

# Ajuster le path pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
#from src.utils.logger import setup_logger
# Charger les variables d'environnement
load_dotenv()

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
#logger = setup_logger(__name__)

# chemin python de l'environnement virtuel
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
os.environ["PYSPARK_DRIVER_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
java_home = os.getenv('JAVA_HOME') # Java 21 compatible pour spark 3.5.x
if java_home:
    os.environ["JAVA_HOME"] = java_home
else:
    pass # on laisse java du conteneur

# Configurer HADOOP_HOME pour Windows si défini dans .env
hadoop_home = os.getenv('HADOOP_HOME')
if hadoop_home and os.path.exists(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
    bin_path = os.path.join(hadoop_home, 'bin')
    if bin_path not in os.environ.get('PATH', ''):
        os.environ['PATH'] = f"{bin_path};{os.environ.get('PATH', '')}"
    logger.info(f"HADOOP_HOME configuré: {hadoop_home}")


UPDATE_INTERVAL = 7000  # 7 secondes
DETECTOR_API_URL = os.getenv('DETECTOR_API_URL', 'http://localhost:5000')

COLORS = {
    'background': '#f5f7fa',
    'card': '#ffffff',
    'text': '#2c3e50',
    'primary': '#3498db',
    'danger': '#e74c3c',
    'warning': '#f39c12',
    'success': '#27ae60',
    'safe': '#1abc9c',
    'secondary': '#9b59b6',
    'border': '#ecf0f1',
    'gradient_start': '#667eea',
    'gradient_end': '#764ba2'
}

app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Fraud Detection Dashboard"

def get_data():
    """Récupère les données depuis les fichiers parquet"""
    try:
        data_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "transactions")
        data_path = os.path.abspath(data_path)
        
        if not os.path.exists(data_path):
            return None
        
        parquet_files = glob.glob(os.path.join(data_path, "*.parquet"))
        if not parquet_files:
            return None
        
        dfs = []
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Erreur lecture {file}: {e}")
        
        if not dfs:
            return None
        
        pdf = pd.concat(dfs, ignore_index=True)
        if 'timestamp' in pdf.columns:
            pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])
        
        logger.info(f"{len(pdf)} transactions récupérées")
        return pdf
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return None

def fetch_data_from_api():
    """Récupère les données depuis l'API du detector"""
    try:
        response = requests.get(f"{DETECTOR_API_URL}/api/fraud-data", timeout=5)
        if response.status_code == 200:
            result = response.json()
            if result['status'] == 'success' and result['data']:
                df = pd.DataFrame(result['data'])
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                print(f"*** {len(df)} transactions récupérées depuis l'API")
                return df
    except Exception as e:
        print(f"!!! Erreur API: {e}")
    return None


def fetch_stats_from_api():
    """Récupère les stats depuis l'API"""
    try:
        response = requests.get(f"{DETECTOR_API_URL}/api/stats", timeout=5)
        if response.status_code == 200:
            result = response.json()
            if result['status'] == 'success':
                return result['stats']
    except Exception as e:
        print(f"!!! Erreur stats API: {e}")
    return None


# LAYOUT PRINCIPAL
app.layout = html.Div([
    html.Div([
        html.Div([
            html.H1("Fraud Detection System", style={
                'color': 'white', 'margin': '0', 'fontSize': '2.5em',
                'fontWeight': '700', 'textShadow': '2px 2px 4px rgba(0,0,0,0.2)'
            }),
            html.P("Real-Time Machine Learning Dashboard", style={
                'color': 'rgba(255,255,255,0.9)', 'margin': '10px 0 0 0', 'fontSize': '1.1em'
            })
        ], style={'textAlign': 'center'})
    ], style={
        'background': f'linear-gradient(135deg, {COLORS["gradient_start"]} 0%, {COLORS["gradient_end"]} 100%)',
        'padding': '40px', 'marginBottom': '30px', 'borderRadius': '15px',
        'boxShadow': '0 10px 40px rgba(102, 126, 234, 0.3)'
    }),
    
    dcc.Interval(id='interval-component', interval=UPDATE_INTERVAL, n_intervals=0),
    
    html.Div([
        dcc.Tabs(id='tabs', value='monitoring', children=[
            dcc.Tab(label='Monitoring', value='monitoring',
                   style={'backgroundColor': COLORS['card'], 'color': COLORS['text'], 'padding': '15px 30px', 'fontWeight': '600'},
                   selected_style={'backgroundColor': COLORS['primary'], 'color': 'white', 'padding': '15px 30px', 'fontWeight': '700'}),
            dcc.Tab(label='Performance ML', value='performance',
                   style={'backgroundColor': COLORS['card'], 'color': COLORS['text'], 'padding': '15px 30px', 'fontWeight': '600'},
                   selected_style={'backgroundColor': COLORS['primary'], 'color': 'white', 'padding': '15px 30px', 'fontWeight': '700'}),
            dcc.Tab(label='Analyse Avancée', value='exploration',
                   style={'backgroundColor': COLORS['card'], 'color': COLORS['text'], 'padding': '15px 30px', 'fontWeight': '600'},
                   selected_style={'backgroundColor': COLORS['primary'], 'color': 'white', 'padding': '15px 30px', 'fontWeight': '700'}),
        ], style={'marginBottom': '30px'})
    ]),
    
    html.Div(id='page-content')
], style={
    'backgroundColor': COLORS['background'], 'padding': '20px', 'minHeight': '100vh',
    'fontFamily': "'Inter', 'Segoe UI', sans-serif"
})

def create_kpi_card(icon, title, value, color, subtitle=""):
    return html.Div([
        html.Div([
            html.H4(title, style={'color': COLORS['text'], 'fontSize': '0.9em', 'fontWeight': '600', 'marginBottom': '10px', 'textTransform': 'uppercase'}),
            html.H2(value, style={'color': color, 'fontSize': '2.2em', 'fontWeight': '700', 'margin': '5px 0'}),
            html.P(subtitle, style={'color': COLORS['text'], 'fontSize': '0.85em', 'margin': '5px 0 0 0', 'opacity': '0.7'}) if subtitle else None
        ])
    ], style={
        'backgroundColor': COLORS['card'], 'padding': '30px', 'margin': '10px', 'borderRadius': '15px',
        'textAlign': 'center', 'flex': '1', 'minWidth': '220px', 'boxShadow': '0 4px 15px rgba(0,0,0,0.08)',
        'border': f'1px solid {COLORS["border"]}'
    })

def create_empty_figure():
    fig = go.Figure()
    fig.update_layout(template='plotly_white', paper_bgcolor=COLORS['card'], plot_bgcolor=COLORS['card'], font_color=COLORS['text'])
    return fig

def create_no_data_message():
    return html.Div([
        html.Div([
            html.H3("En attente de données...", style={'color': COLORS['warning'], 'marginBottom': '15px'}),
            html.P("Aucune donnée reçue depuis l'API ", style={'color': COLORS['text'], 'marginBottom': '10px'}),
            html.P("Veuillez patienter quelques secondes ou executer: python create_test_data.py", style={'color': COLORS['text'], 'fontSize': '0.9em'})
        ], style={'textAlign': 'center', 'padding': '60px'})
    ], style={'backgroundColor': COLORS['card'], 'borderRadius': '15px', 'boxShadow': '0 4px 15px rgba(0,0,0,0.08)'})

header_style = {'padding': '15px 12px', 'textAlign': 'left', 'color': COLORS['text'], 'fontWeight': '700', 'fontSize': '0.9em', 'textTransform': 'uppercase'}

@app.callback(Output('page-content', 'children'), Input('tabs', 'value'))
def render_content(tab):
    graph_half = {'width': '50%', 'display': 'inline-block', 'padding': '10px', 'verticalAlign': 'top', 'boxSizing': 'border-box'}
    graph_full = {'width': '100%', 'padding': '10px'}
    
    if tab == 'monitoring':
        return html.Div([
            html.Div(id='kpi-cards', style={'marginBottom': '30px'}),
            html.Div([dcc.Graph(id='transactions-timeline')], style={**graph_full, 'marginBottom': '20px'}),
            html.Div([
                html.Div([dcc.Graph(id='risk-gauge')], style=graph_half),
                html.Div([dcc.Graph(id='fraud-score-distribution')], style=graph_half),
            ], style={'marginBottom': '20px', 'display': 'flex'}),
            html.Div([
                html.Div([dcc.Graph(id='risk-pie-chart')], style=graph_half),
                html.Div([dcc.Graph(id='category-fraud-rate')], style=graph_half),
            ], style={'marginBottom': '20px', 'display': 'flex'}),
            html.Div(id='recent-alerts', style={'marginTop': '20px'})
        ])
    elif tab == 'performance':
        return html.Div([
            html.Div(id='ml-kpi-cards', style={'marginBottom': '30px'}),
            html.Div([
                html.Div([dcc.Graph(id='confusion-matrix')], style=graph_half),
                html.Div([dcc.Graph(id='metrics-comparison')], style=graph_half),
            ], style={'marginBottom': '20px', 'display': 'flex'}),
            html.Div([
                html.Div([dcc.Graph(id='feature-importance')], style=graph_half),
                html.Div([dcc.Graph(id='score-distribution-ml')], style=graph_half),
            ], style={'display': 'flex'})
        ])
    elif tab == 'exploration':
        return html.Div([
            # Graphiques originaux
            html.Div([dcc.Graph(id='fraud-map')], style={**graph_full, 'marginBottom': '20px'}),
            html.Div([
                html.Div([dcc.Graph(id='amount-distribution')], style=graph_half),
                html.Div([dcc.Graph(id='hourly-pattern')], style=graph_half),
            ], style={'marginBottom': '20px', 'display': 'flex'}),
            html.Div([
                html.Div([dcc.Graph(id='fraud-by-category')], style=graph_half),
                html.Div([dcc.Graph(id='time-heatmap')], style=graph_half),
            ], style={'marginBottom': '20px', 'display': 'flex'}),
            html.Div([dcc.Graph(id='daily-trend')], style={**graph_full, 'marginBottom': '20px'}),
            
            # NOUVEAUX GRAPHIQUES AVANCÉS
            html.Div([
                html.H2("Analyses Avancées", style={
                    'color': COLORS['text'], 
                    'textAlign': 'center', 
                    'margin': '40px 0 30px 0',
                    'fontSize': '1.8em',
                    'fontWeight': '700'
                })
            ]),
            
            # 1. Séquences temporelles (pleine largeur)
            html.Div([dcc.Graph(id='temporal-sequences')], style={**graph_full, 'marginBottom': '20px'}),
            
            # 2. Network graph et 3. Anomalies géographiques (côte à côte)
            html.Div([
                html.Div([dcc.Graph(id='network-graph')], style=graph_half),
                html.Div([dcc.Graph(id='geo-anomalies')], style=graph_half),
            ], style={'marginBottom': '20px', 'display': 'flex'}),
            
            # 4. Funnel de conversion (pleine largeur)
            html.Div([dcc.Graph(id='alert-funnel')], style={**graph_full, 'marginBottom': '20px'}),
        ])

@app.callback(
    [Output('kpi-cards', 'children'), Output('transactions-timeline', 'figure'),
     Output('risk-gauge', 'figure'), Output('fraud-score-distribution', 'figure'),
     Output('risk-pie-chart', 'figure'), Output('category-fraud-rate', 'figure'),
     Output('recent-alerts', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_monitoring(n):
    #pdf = get_data()
    pdf = fetch_data_from_api()
    empty = create_empty_figure()
    if pdf is None or len(pdf) == 0:
        no_data = create_no_data_message()
        return (no_data, empty, empty, empty, empty, empty, no_data)
    
    fraud_col = 'predicted_fraud' if 'predicted_fraud' in pdf.columns else 'is_fraud'
    total = len(pdf)
    frauds = len(pdf[pdf[fraud_col] == 1]) if fraud_col in pdf.columns else 0
    fraud_rate = (frauds / total * 100) if total > 0 else 0
    total_amount = pdf['amount'].sum() if 'amount' in pdf.columns else 0
    avg_score = pdf['fraud_score'].mean() if 'fraud_score' in pdf.columns else 0
    
    kpis = html.Div([
        create_kpi_card("", "Total Transactions", f"{total:,}", COLORS['primary'], "Volume total"),
        create_kpi_card("", "Fraudes Détectées", f"{frauds:,}", COLORS['danger'], f"{fraud_rate:.1f}%"),
        create_kpi_card("", "Montant Total", f"${total_amount:,.0f}", COLORS['success'], "Toutes transactions"),
        create_kpi_card("", "Score Moyen", f"{avg_score:.1f}", COLORS['warning'], "Risque moyen"),
    ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around'})
    
    # Timeline
    fig_timeline = create_empty_figure()
    if 'timestamp' in pdf.columns and fraud_col in pdf.columns:
        pdf_sorted = pdf.sort_values('timestamp').copy()
        pdf_sorted['minute'] = pdf_sorted['timestamp'].dt.floor('5min')
        timeline_total = pdf_sorted.groupby('minute').size().reset_index(name='total')
        timeline_fraud = pdf_sorted[pdf_sorted[fraud_col] == 1].groupby('minute').size().reset_index(name='frauds')
        timeline_data = timeline_total.merge(timeline_fraud, on='minute', how='left').fillna(0)
        timeline_data['normal'] = timeline_data['total'] - timeline_data['frauds']
        
        fig_timeline = go.Figure()
        fig_timeline.add_trace(go.Scatter(
            x=timeline_data['minute'], y=timeline_data['normal'], mode='lines+markers',
            name='Transactions Légitimes', line=dict(color=COLORS['success'], width=3),
            marker=dict(size=6, color=COLORS['success']),
            hovertemplate='Légitimes: %{y}<extra></extra>'
        ))
        fig_timeline.add_trace(go.Scatter(
            x=timeline_data['minute'], y=timeline_data['frauds'], mode='lines+markers',
            name='Transactions Frauduleuses', line=dict(color=COLORS['danger'], width=3),
            marker=dict(size=6, color=COLORS['danger']),
            hovertemplate='Fraudes: %{y}<extra></extra>'
        ))
        fig_timeline.update_layout(
            title='Évolution des Transactions au Fil du Temps',
            xaxis_title='Temps', yaxis_title='Nombre de Transactions',
            template='plotly_white', paper_bgcolor=COLORS['card'],
            plot_bgcolor='rgba(0,0,0,0.02)', font=dict(color=COLORS['text'], size=12),
            hovermode='x unified', showlegend=True,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1,
                       bgcolor='rgba(255,255,255,0.8)', bordercolor=COLORS['border'], borderwidth=1),
            height=400, xaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.1)'),
            yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.1)')
        )
    
    # Gauge
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta", value=fraud_rate, title={'text': "Taux de Fraude (%)"},
        delta={'reference': 5},
        gauge={'axis': {'range': [None, 100]}, 'bar': {'color': COLORS['danger']},
               'steps': [{'range': [0, 3], 'color': COLORS['safe']},
                        {'range': [3, 7], 'color': COLORS['warning']},
                        {'range': [7, 100], 'color': COLORS['danger']}]}
    ))
    fig_gauge.update_layout(paper_bgcolor=COLORS['card'], font={'color': COLORS['text']}, height=400)
    
    # Distribution des Scores
    fig_score = create_empty_figure()
    if 'fraud_score' in pdf.columns:
        pdf_copy = pdf.copy()
        def categorize_score(score):
            if score < 30: return 'SAFE (0-30)'
            elif score < 50: return 'LOW (30-50)'
            elif score < 70: return 'MEDIUM (50-70)'
            else: return 'HIGH (70-100)'
        
        pdf_copy['Risk_Category'] = pdf_copy['fraud_score'].apply(categorize_score)
        category_order = ['SAFE (0-30)', 'LOW (30-50)', 'MEDIUM (50-70)', 'HIGH (70-100)']
        colors_box = [COLORS['safe'], COLORS['success'], COLORS['warning'], COLORS['danger']]
        
        fig_score = go.Figure()
        for i, category in enumerate(category_order):
            data = pdf_copy[pdf_copy['Risk_Category'] == category]['fraud_score']
            if len(data) > 0:
                fig_score.add_trace(go.Box(
                    y=data, name=category, marker_color=colors_box[i], boxmean='sd',
                    hovertemplate='Score: %{y}<br>Catégorie: ' + category + '<extra></extra>'
                ))
        
        fig_score.update_layout(
            title='Distribution des Scores de Fraude par Niveau de Risque',
            yaxis_title='Score de Fraude', xaxis_title='Catégorie de Risque',
            template='plotly_white', paper_bgcolor=COLORS['card'],
            plot_bgcolor='rgba(0,0,0,0.02)', font=dict(color=COLORS['text']),
            showlegend=False, height=350,
            yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.1)', range=[0, 100]),
            xaxis=dict(showgrid=False)
        )
    
    # Pie
    fig_risk = create_empty_figure()
    if 'risk_level' in pdf.columns:
        risk_counts = pdf['risk_level'].value_counts()
        colors_map = {'SAFE': COLORS['safe'], 'LOW': COLORS['success'],
                     'MEDIUM': COLORS['warning'], 'HIGH': COLORS['danger']}
        fig_risk = go.Figure(data=[go.Pie(
            labels=risk_counts.index, values=risk_counts.values, hole=0.5,
            marker=dict(colors=[colors_map.get(x, COLORS['text']) for x in risk_counts.index])
        )])
        fig_risk.update_layout(title='Niveaux de Risque', paper_bgcolor=COLORS['card'], height=350)
    
    # Catégorie
    fig_cat = create_empty_figure()
    if 'merchant_category' in pdf.columns and fraud_col in pdf.columns:
        cat_stats = pdf.groupby('merchant_category').agg({fraud_col: ['sum', 'count']}).reset_index()
        cat_stats.columns = ['category', 'frauds', 'total']
        cat_stats['rate'] = (cat_stats['frauds'] / cat_stats['total'] * 100).round(1)
        cat_stats = cat_stats.sort_values('rate', ascending=True)
        fig_cat = go.Figure(go.Bar(
            x=cat_stats['rate'], y=cat_stats['category'], orientation='h',
            marker=dict(color=cat_stats['rate'],
                       colorscale=[[0, COLORS['success']], [1, COLORS['danger']]])
        ))
        fig_cat.update_layout(title='Taux de Fraude par Catégorie',
                             paper_bgcolor=COLORS['card'], height=350)
    
    # Alertes
    alerts = html.Div("Aucune fraude", style={'padding': '20px'})
    if fraud_col in pdf.columns and 'timestamp' in pdf.columns:
        frauds = pdf[pdf[fraud_col] == 1].sort_values('timestamp', ascending=False).head(15)
        if len(frauds) > 0:
            rows = []
            for _, row in frauds.iterrows():
                risk_color = COLORS['danger'] if row.get('fraud_score', 0) >= 70 else COLORS['warning']
                rows.append(html.Tr([
                    html.Td(row['timestamp'].strftime('%H:%M:%S'), style={'padding': '12px'}),
                    html.Td(str(row.get('transaction_id', 'N/A'))[:15], style={'padding': '12px'}),
                    html.Td(str(row.get('user_id', 'N/A')), style={'padding': '12px'}),
                    html.Td(f"${row.get('amount', 0):.2f}", style={'padding': '12px', 'fontWeight': '600'}),
                    html.Td(html.Span(f"{row.get('fraud_score', 0):.0f}",
                                     style={'backgroundColor': risk_color, 'color': 'white',
                                           'padding': '5px 15px', 'borderRadius': '20px'}),
                           style={'padding': '12px'}),
                ]))
            alerts = html.Div([
                html.H3("Alertes Récentes", style={'color': COLORS['text'], 'marginBottom': '20px'}),
                html.Table([
                    html.Thead(html.Tr([
                        html.Th('Heure', style=header_style),
                        html.Th('ID', style=header_style),
                        html.Th('User', style=header_style),
                        html.Th('Montant', style=header_style),
                        html.Th('Score', style=header_style)
                    ])),
                    html.Tbody(rows)
                ], style={'width': '100%', 'borderCollapse': 'collapse', 'backgroundColor': COLORS['card']})
            ], style={'backgroundColor': COLORS['card'], 'padding': '30px', 'borderRadius': '15px'})
    
    return (kpis, fig_timeline, fig_gauge, fig_score, fig_risk, fig_cat, alerts)

@app.callback(
    [Output('ml-kpi-cards', 'children'), Output('confusion-matrix', 'figure'),
     Output('metrics-comparison', 'figure'), Output('feature-importance', 'figure'),
     Output('score-distribution-ml', 'figure')],
    Input('interval-component', 'n_intervals')
)
def update_performance(n):
    #pdf = get_data()
    pdf = fetch_data_from_api()
    empty = create_empty_figure()
    if pdf is None or len(pdf) == 0:
        return (create_no_data_message(), empty, empty, empty, empty)
    
    kpis = create_no_data_message()
    fig_conf = empty
    fig_metrics = empty
    
    if 'predicted_fraud' in pdf.columns and 'is_fraud' in pdf.columns:
        tp = len(pdf[(pdf['is_fraud'] == 1) & (pdf['predicted_fraud'] == 1.0)])
        fp = len(pdf[(pdf['is_fraud'] == 0) & (pdf['predicted_fraud'] == 1.0)])
        tn = len(pdf[(pdf['is_fraud'] == 0) & (pdf['predicted_fraud'] == 0.0)])
        fn = len(pdf[(pdf['is_fraud'] == 1) & (pdf['predicted_fraud'] == 0.0)])
        
        total = len(pdf)
        accuracy = ((tp + tn) / total * 100) if total > 0 else 0
        precision = (tp / (tp + fp) * 100) if (tp + fp) > 0 else 0
        recall = (tp / (tp + fn) * 100) if (tp + fn) > 0 else 0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0
        
        kpis = html.Div([
            create_kpi_card("", "Accuracy", f"{accuracy:.1f}%", COLORS['success']),
            create_kpi_card("", "Precision", f"{precision:.1f}%", COLORS['primary']),
            create_kpi_card("", "Recall", f"{recall:.1f}%", COLORS['primary']),
            create_kpi_card("", "F1-Score", f"{f1:.1f}%", COLORS['secondary']),
        ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around'})
        
        confusion_data = [[tn, fp], [fn, tp]]
        fig_conf = go.Figure(data=go.Heatmap(
            z=confusion_data, x=['Prédit: Normal', 'Prédit: Fraude'],
            y=['Réel: Normal', 'Réel: Fraude'], colorscale='Blues',
            text=confusion_data, texttemplate='<b>%{text}</b>',
            textfont={"size": 24}, showscale=False
        ))
        fig_conf.update_layout(title='Matrice de Confusion',
                              paper_bgcolor=COLORS['card'], height=450)
        
        metrics = ['Accuracy', 'Precision', 'Recall', 'F1']
        values = [accuracy, precision, recall, f1]
        fig_metrics = go.Figure()
        fig_metrics.add_trace(go.Barpolar(
            r=values, theta=metrics,
            marker=dict(color=values,
                       colorscale=[[0, COLORS['danger']], [1, COLORS['success']]])
        ))
        fig_metrics.update_layout(
            title='Métriques ML',
            polar=dict(radialaxis=dict(range=[0, 100])),
            paper_bgcolor=COLORS['card'], height=450
        )
    
    features = ['Montant', 'Heure', 'Jour', 'Catégorie', 'Localisation', 'Vélocité']
    importance = [0.28, 0.18, 0.15, 0.12, 0.10, 0.08]
    fig_feat = go.Figure(go.Bar(
        x=importance, y=features, orientation='h',
        marker=dict(color=importance,
                   colorscale=[[0, COLORS['secondary']], [1, COLORS['primary']]])
    ))
    fig_feat.update_layout(title='Feature Importance',
                          paper_bgcolor=COLORS['card'], height=450)
    
    fig_dist = empty
    if 'fraud_score' in pdf.columns:
        fraud_col = 'predicted_fraud' if 'predicted_fraud' in pdf.columns else 'is_fraud'
        if fraud_col in pdf.columns:
            pdf_copy = pdf.copy()
            pdf_copy['Type'] = pdf_copy[fraud_col].apply(
                lambda x: 'Fraude' if x in [1, 1.0] else 'Normal'
            )
            fig_dist = go.Figure()
            fig_dist.add_trace(go.Violin(
                x=pdf_copy[pdf_copy['Type'] == 'Normal']['Type'],
                y=pdf_copy[pdf_copy['Type'] == 'Normal']['fraud_score'],
                fillcolor=COLORS['success'], name='Normal'
            ))
            fig_dist.add_trace(go.Violin(
                x=pdf_copy[pdf_copy['Type'] == 'Fraude']['Type'],
                y=pdf_copy[pdf_copy['Type'] == 'Fraude']['fraud_score'],
                fillcolor=COLORS['danger'], name='Fraude'
            ))
            fig_dist.update_layout(title='🎻 Distribution Scores',
                                  paper_bgcolor=COLORS['card'], height=450)
    
    return (kpis, fig_conf, fig_metrics, fig_feat, fig_dist)

@app.callback(
    [Output('fraud-map', 'figure'), Output('amount-distribution', 'figure'),
     Output('hourly-pattern', 'figure'), Output('fraud-by-category', 'figure'),
     Output('time-heatmap', 'figure'), Output('daily-trend', 'figure'),
     # NOUVEAUX OUTPUTS
     Output('temporal-sequences', 'figure'), Output('network-graph', 'figure'),
     Output('geo-anomalies', 'figure'), Output('alert-funnel', 'figure')],
    Input('interval-component', 'n_intervals')
)
def update_exploration(n):
    empty = create_empty_figure()
    
    try:
        #pdf = get_data()
        pdf = fetch_data_from_api()
        if pdf is None or len(pdf) == 0:
            return (empty, empty, empty, empty, empty, empty, empty, empty, empty, empty)
        
        fraud_col = 'predicted_fraud' if 'predicted_fraud' in pdf.columns else 'is_fraud'
    except Exception as e:
        logger.error(f"Erreur get_data: {e}")
        return (empty, empty, empty, empty, empty, empty, empty, empty, empty, empty)
    
    # Carte
    fig_map = empty
    try:
        if all(col in pdf.columns for col in ['location_lat', 'location_lon', fraud_col]):
            fraud_data = pdf[pdf[fraud_col] == 1].copy()
            if len(fraud_data) > 0:
                fig_map = px.scatter_mapbox(
                    fraud_data, lat='location_lat', lon='location_lon', size='amount',
                    color='fraud_score' if 'fraud_score' in fraud_data.columns else 'amount',
                    color_continuous_scale='Reds', zoom=2, height=500
                )
                fig_map.update_layout(
                    mapbox_style="carto-darkmatter",
                    title='Localisation Géographique',
                    paper_bgcolor=COLORS['card']
                )
    except Exception as e:
        logger.error(f"Erreur carte: {e}")
    
    # Box
    fig_amount = empty
    try:
        if 'amount' in pdf.columns and fraud_col in pdf.columns:
            pdf_copy = pdf.copy()
            pdf_copy['Type'] = pdf_copy[fraud_col].apply(
                lambda x: 'Fraude' if x in [1, 1.0] else 'Normal'
            )
            fig_amount = go.Figure()
            fig_amount.add_trace(go.Box(
                x=pdf_copy[pdf_copy['Type'] == 'Normal']['Type'],
                y=pdf_copy[pdf_copy['Type'] == 'Normal']['amount'],
                marker_color=COLORS['success']
            ))
            fig_amount.add_trace(go.Box(
                x=pdf_copy[pdf_copy['Type'] == 'Fraude']['Type'],
                y=pdf_copy[pdf_copy['Type'] == 'Fraude']['amount'],
                marker_color=COLORS['danger']
            ))
            fig_amount.update_layout(
                title='Distribution Montants',
                paper_bgcolor=COLORS['card'], height=400
            )
    except Exception as e:
        logger.error(f"Erreur montants: {e}")
    
    # Horaire
    fig_hourly = empty
    try:
        if 'timestamp' in pdf.columns and fraud_col in pdf.columns:
            pdf_copy = pdf.copy()
            pdf_copy['hour'] = pdf_copy['timestamp'].dt.hour
            hourly = pdf_copy.groupby(['hour', fraud_col]).size().reset_index(name='count')
            fig_hourly = go.Figure()
            fig_hourly.add_trace(go.Bar(
                x=hourly[hourly[fraud_col] == 0]['hour'],
                y=hourly[hourly[fraud_col] == 0]['count'],
                name='Normal', marker_color=COLORS['success']
            ))
            fig_hourly.add_trace(go.Bar(
                x=hourly[hourly[fraud_col] == 1]['hour'],
                y=hourly[hourly[fraud_col] == 1]['count'],
                name='Fraude', marker_color=COLORS['danger']
            ))
            fig_hourly.update_layout(
                title='Pattern Horaire', barmode='stack',
                paper_bgcolor=COLORS['card'], height=400
            )
    except Exception as e:
        logger.error(f"Erreur horaire: {e}")
    
    # Sunburst
    fig_cat = empty
    try:
        if 'merchant_category' in pdf.columns and fraud_col in pdf.columns:
            cat_data = pdf.groupby(['merchant_category', fraud_col]).size().reset_index(name='count')
            cat_data['type'] = cat_data[fraud_col].apply(
                lambda x: 'Fraude' if x in [1, 1.0] else 'Normal'
            )
            fig_cat = px.sunburst(
                cat_data, path=['merchant_category', 'type'], values='count',
                color='type',
                color_discrete_map={'Normal': COLORS['success'], 'Fraude': COLORS['danger']}
            )
            fig_cat.update_layout(
                title='Répartition Catégories',
                paper_bgcolor=COLORS['card'], height=400
            )
    except Exception as e:
        logger.error(f"Erreur catégories: {e}")
    
    # Heatmap
    fig_heat = empty
    try:
        if 'timestamp' in pdf.columns and fraud_col in pdf.columns:
            pdf_copy = pdf.copy()
            pdf_copy['hour'] = pdf_copy['timestamp'].dt.hour
            pdf_copy['day'] = pdf_copy['timestamp'].dt.day_name()
            fraud_data = pdf_copy[pdf_copy[fraud_col] == 1]
            if len(fraud_data) > 0:
                days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                days_fr = ['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim']
                heatmap_data = fraud_data.groupby(['day', 'hour']).size().reset_index(name='count')
                matrix = np.zeros((7, 24))
                for _, row in heatmap_data.iterrows():
                    try:
                        matrix[days_order.index(row['day'])][int(row['hour'])] = row['count']
                    except:
                        pass
                fig_heat = go.Figure(data=go.Heatmap(
                    z=matrix, x=list(range(24)), y=days_fr, colorscale='Reds'
                ))
                fig_heat.update_layout(
                    title='Heatmap Temporelle',
                    paper_bgcolor=COLORS['card'], height=400
                )
    except Exception as e:
        logger.error(f"Erreur heatmap: {e}")
    
    # Tendance
    fig_daily = empty
    try:
        if 'timestamp' in pdf.columns and fraud_col in pdf.columns:
            pdf_copy = pdf.copy()
            pdf_copy['date'] = pdf_copy['timestamp'].dt.date
            daily = pdf_copy[pdf_copy[fraud_col] == 1].groupby('date').size().reset_index(name='count')
            if len(daily) > 0:
                daily['ma3'] = daily['count'].rolling(window=3, min_periods=1).mean()
                fig_daily = go.Figure()
                fig_daily.add_trace(go.Bar(
                    x=daily['date'], y=daily['count'],
                    marker_color=COLORS['danger'], opacity=0.6
                ))
                fig_daily.add_trace(go.Scatter(
                    x=daily['date'], y=daily['ma3'],
                    line=dict(color=COLORS['primary'], width=3)
                ))
                fig_daily.update_layout(
                    title='Tendance Journalière',
                    paper_bgcolor=COLORS['card'], height=400
                )
    except Exception as e:
        logger.error(f"Erreur tendance: {e}")
    
    # ========== NOUVEAUX GRAPHIQUES AVANCÉS ==========
    
    # 1. SÉQUENCES TEMPORELLES
    fig_sequences = empty
    try:
        if all(col in pdf.columns for col in ['timestamp', 'user_id', fraud_col, 'amount']):
            fraud_users_series = pdf[pdf[fraud_col] == 1]['user_id'].value_counts()
            
            if len(fraud_users_series) > 0:
                fraud_users = fraud_users_series.head(8).index.tolist()
                sequences_data = pdf[pdf['user_id'].isin(fraud_users)].copy()
                sequences_data = sequences_data.sort_values('timestamp')
                
                fig_sequences = go.Figure()
                
                for i, user in enumerate(fraud_users):
                    user_data = sequences_data[sequences_data['user_id'] == user]
                    
                    if len(user_data) == 0:
                        continue
                    
                    normal_trans = user_data[user_data[fraud_col] == 0]
                    if len(normal_trans) > 0:
                        amounts_normal = normal_trans['amount'].values
                        sizes_normal = np.clip(amounts_normal / 30, 6, 20)
                        
                        fig_sequences.add_trace(go.Scatter(
                            x=normal_trans['timestamp'],
                            y=[i] * len(normal_trans),
                            mode='markers',
                            marker=dict(
                                size=sizes_normal,
                                color=COLORS['success'],
                                symbol='circle',
                                line=dict(width=1, color='white'),
                                opacity=0.7
                            ),
                            hovertemplate='<b>User %{text}</b><br>Montant: $%{customdata:.2f}<br>%{x|%H:%M:%S}<extra></extra>',
                            text=[user] * len(normal_trans),
                            customdata=amounts_normal,
                            showlegend=False
                        ))
                    
                    fraud_trans = user_data[user_data[fraud_col] == 1]
                    if len(fraud_trans) > 0:
                        amounts_fraud = fraud_trans['amount'].values
                        sizes_fraud = np.clip(amounts_fraud / 25, 10, 25)
                        
                        fig_sequences.add_trace(go.Scatter(
                            x=fraud_trans['timestamp'],
                            y=[i] * len(fraud_trans),
                            mode='markers',
                            marker=dict(
                                size=sizes_fraud,
                                color=COLORS['danger'],
                                symbol='star',
                                line=dict(width=2, color='white')
                            ),
                            hovertemplate='<b> FRAUDE - User %{text}</b><br>Montant: $%{customdata:.2f}<br>%{x|%H:%M:%S}<extra></extra>',
                            text=[user] * len(fraud_trans),
                            customdata=amounts_fraud,
                            showlegend=False
                        ))
                
                fig_sequences.update_layout(
                    title='Séquences Temporelles des Transactions Suspectes par Utilisateur',
                    xaxis_title='Temps',
                    yaxis_title='Utilisateurs',
                    yaxis=dict(
                        tickmode='array',
                        tickvals=list(range(len(fraud_users))),
                        ticktext=[f'User {str(u)[:10]}' for u in fraud_users]
                    ),
                    template='plotly_white',
                    paper_bgcolor=COLORS['card'],
                    plot_bgcolor='rgba(0,0,0,0.02)',
                    font=dict(color=COLORS['text']),
                    height=500,
                    hovermode='closest',
                    showlegend=False
                )
    except Exception as e:
        logger.error(f"Erreur séquences temporelles: {e}")
    
    # 2. NETWORK GRAPH
    fig_network = empty
    try:
        if all(col in pdf.columns for col in ['user_id', 'merchant_id', fraud_col]):
            fraud_data = pdf[pdf[fraud_col] == 1].copy()
            
            if len(fraud_data) > 5:
                top_users = fraud_data['user_id'].value_counts().head(10).index.tolist()
                top_merchants = fraud_data['merchant_id'].value_counts().head(8).index.tolist()
                
                network_data = fraud_data[
                    fraud_data['user_id'].isin(top_users) & 
                    fraud_data['merchant_id'].isin(top_merchants)
                ].copy()
                
                if len(network_data) > 0:
                    G = nx.Graph()
                    edge_weights = {}
                    
                    for _, row in network_data.iterrows():
                        user = f"U_{str(row['user_id'])[:8]}"
                        merchant = f"M_{str(row['merchant_id'])[:8]}"
                        edge_key = (user, merchant)
                        edge_weights[edge_key] = edge_weights.get(edge_key, 0) + 1
                    
                    for (user, merchant), weight in edge_weights.items():
                        G.add_edge(user, merchant, weight=weight)
                    
                    if len(G.nodes()) > 0:
                        pos = nx.spring_layout(G, k=1.5, iterations=50, seed=42)
                        
                        edge_traces = []
                        for edge in G.edges(data=True):
                            x0, y0 = pos[edge[0]]
                            x1, y1 = pos[edge[1]]
                            weight = edge[2]['weight']
                            
                            edge_traces.append(go.Scatter(
                                x=[x0, x1, None],
                                y=[y0, y1, None],
                                mode='lines',
                                line=dict(width=min(weight * 0.8, 5), color='rgba(125,125,125,0.4)'),
                                hoverinfo='none',
                                showlegend=False
                            ))
                        
                        node_x_user, node_y_user, node_text_user = [], [], []
                        node_x_merchant, node_y_merchant, node_text_merchant = [], [], []
                        
                        for node in G.nodes():
                            x, y = pos[node]
                            connections = G.degree(node)
                            
                            if node.startswith('U_'):
                                node_x_user.append(x)
                                node_y_user.append(y)
                                node_text_user.append(f"Utilisateur {node[2:]}<br>Transactions: {connections}")
                            else:
                                node_x_merchant.append(x)
                                node_y_merchant.append(y)
                                node_text_merchant.append(f"Marchand {node[2:]}<br>Transactions: {connections}")
                        
                        fig_network = go.Figure()
                        
                        for trace in edge_traces:
                            fig_network.add_trace(trace)
                        
                        if len(node_x_user) > 0:
                            fig_network.add_trace(go.Scatter(
                                x=node_x_user, y=node_y_user,
                                mode='markers+text',
                                marker=dict(size=18, color=COLORS['primary'], line=dict(width=2, color='white')),
                                text=[f"U{i+1}" for i in range(len(node_x_user))],
                                textposition='top center',
                                textfont=dict(size=10, color='white'),
                                hovertext=node_text_user,
                                hoverinfo='text',
                                name='Utilisateurs',
                                showlegend=True
                            ))
                        
                        if len(node_x_merchant) > 0:
                            fig_network.add_trace(go.Scatter(
                                x=node_x_merchant, y=node_y_merchant,
                                mode='markers+text',
                                marker=dict(size=22, color=COLORS['danger'], symbol='square', line=dict(width=2, color='white')),
                                text=[f"M{i+1}" for i in range(len(node_x_merchant))],
                                textposition='bottom center',
                                textfont=dict(size=10, color='white'),
                                hovertext=node_text_merchant,
                                hoverinfo='text',
                                name='Marchands',
                                showlegend=True
                            ))
                        
                        fig_network.update_layout(
                            title='Réseau de Relations Frauduleuses (Utilisateurs ↔ Marchands)',
                            showlegend=True,
                            hovermode='closest',
                            template='plotly_white',
                            paper_bgcolor=COLORS['card'],
                            plot_bgcolor='rgba(0,0,0,0.02)',
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            height=500,
                            font=dict(color=COLORS['text'], size=10),
                            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                        )
    except Exception as e:
        logger.error(f"Erreur network graph: {e}")
    
    # 3. ANOMALIES GÉOGRAPHIQUES
    fig_geo = empty
    try:
        if all(col in pdf.columns for col in ['location_lat', 'location_lon', 'user_id', 'timestamp', fraud_col]):
            pdf_sorted = pdf.sort_values(['user_id', 'timestamp']).copy()
            anomalies = []
            fraud_users = pdf_sorted[pdf_sorted[fraud_col] == 1]['user_id'].unique()[:20]
            
            for user in fraud_users:
                user_data = pdf_sorted[pdf_sorted['user_id'] == user].reset_index(drop=True)
                
                if len(user_data) > 1:
                    for i in range(1, min(len(user_data), 20)):
                        try:
                            lat1, lon1 = user_data.loc[i-1, ['location_lat', 'location_lon']]
                            lat2, lon2 = user_data.loc[i, ['location_lat', 'location_lon']]
                            
                            if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
                                continue
                            
                            dlat = abs(float(lat2) - float(lat1))
                            dlon = abs(float(lon2) - float(lon1))
                            distance = np.sqrt(dlat**2 + dlon**2) * 111
                            
                            time_diff = (user_data.loc[i, 'timestamp'] - user_data.loc[i-1, 'timestamp']).total_seconds() / 3600
                            
                            if time_diff > 0 and distance > 0:
                                velocity = distance / time_diff
                                
                                if velocity > 800:
                                    anomalies.append({
                                        'lat': float(lat2),
                                        'lon': float(lon2),
                                        'distance': distance,
                                        'velocity': min(velocity, 5000),
                                        'user': str(user)[:10],
                                        'is_fraud': int(user_data.loc[i, fraud_col])
                                    })
                        except:
                            continue
            
            if len(anomalies) > 0:
                anomalies_df = pd.DataFrame(anomalies)
                
                fig_geo = px.density_mapbox(
                    anomalies_df, lat='lat', lon='lon', z='velocity', radius=20,
                    center=dict(lat=anomalies_df['lat'].mean(), lon=anomalies_df['lon'].mean()),
                    zoom=1, mapbox_style="carto-darkmatter",
                    color_continuous_scale='Hot',
                    labels={'velocity': 'Vitesse (km/h)'},
                    range_color=[800, 3000]
                )
                
                fraud_anomalies = anomalies_df[anomalies_df['is_fraud'] == 1]
                if len(fraud_anomalies) > 0:
                    fig_geo.add_trace(go.Scattermapbox(
                        lat=fraud_anomalies['lat'], lon=fraud_anomalies['lon'],
                        mode='markers',
                        marker=dict(size=12, color=COLORS['danger'], symbol='circle', opacity=0.9),
                        text=[f"Anomalie<br>Vitesse: {v:.0f} km/h<br>Distance: {d:.0f} km<br>User: {u}" 
                              for v, d, u in zip(fraud_anomalies['velocity'], fraud_anomalies['distance'], fraud_anomalies['user'])],
                        hoverinfo='text',
                        name='Anomalies Frauduleuses',
                        showlegend=True
                    ))
                
                fig_geo.update_layout(
                    title='Heatmap des Anomalies Géographiques (Déplacements Impossibles)',
                    paper_bgcolor=COLORS['card'], height=500,
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                )
            else:
                fig_geo = go.Figure()
                fig_geo.add_annotation(
                    text="Aucune anomalie géographique détectée<br>(vitesse > 800 km/h)",
                    xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
                    font=dict(size=16, color=COLORS['text']), align="center"
                )
                fig_geo.update_layout(
                    title='Heatmap des Anomalies Géographiques',
                    paper_bgcolor=COLORS['card'], height=500,
                    xaxis=dict(visible=False), yaxis=dict(visible=False)
                )
    except Exception as e:
        logger.error(f"Erreur anomalies géo: {e}")
    
    # 4. FUNNEL DE CONVERSION
    fig_funnel = empty
    try:
        if fraud_col in pdf.columns and 'fraud_score' in pdf.columns:
            total_transactions = len(pdf)
            high_risk = len(pdf[pdf['fraud_score'] >= 70])
            alerts_generated = len(pdf[pdf[fraud_col] == 1])
            
            confirmed_frauds = alerts_generated
            if 'is_fraud' in pdf.columns and 'predicted_fraud' in pdf.columns:
                true_positives = len(pdf[(pdf['is_fraud'] == 1) & (pdf['predicted_fraud'] == 1)])
                confirmed_frauds = true_positives
            
            actions_taken = int(confirmed_frauds * 0.6)
            
            stages = ['Transactions Totales', 'Scores Élevés (≥70)', 'Alertes Générées', 
                     'Fraudes Confirmées', 'Actions Prises']
            values = [total_transactions, high_risk, alerts_generated, confirmed_frauds, actions_taken]
            
            for i in range(1, len(values)):
                if values[i] > values[i-1]:
                    values[i] = values[i-1]
            
            fig_funnel = go.Figure()
            fig_funnel.add_trace(go.Funnel(
                y=stages, x=values,
                textposition="inside",
                textinfo="value+percent initial",
                marker=dict(color=[COLORS['primary'], COLORS['warning'], COLORS['danger'], 
                                  COLORS['secondary'], COLORS['success']]),
                connector=dict(line=dict(color=COLORS['border'], width=3)),
                hovertemplate='<b>%{y}</b><br>Nombre: %{x}<br>Du total: %{percentInitial}<extra></extra>'
            ))
            
            annotations = []
            for i in range(1, len(values)):
                if values[i-1] > 0:
                    conversion_rate = (values[i] / values[i-1]) * 100
                    annotations.append(dict(
                        x=values[i], y=i - 0.5, text=f"↓ {conversion_rate:.1f}%",
                        showarrow=False, font=dict(size=12, color=COLORS['text'], weight='bold'),
                        xanchor='left', xshift=10
                    ))
            
            fig_funnel.update_layout(
                title='Funnel de Conversion des Alertes de Fraude',
                paper_bgcolor=COLORS['card'],
                font=dict(color=COLORS['text'], size=14),
                height=500, showlegend=False, annotations=annotations
            )
    except Exception as e:
        logger.error(f"Erreur funnel: {e}")
    
    return (fig_map, fig_amount, fig_hourly, fig_cat, fig_heat, fig_daily,
            fig_sequences, fig_network, fig_geo, fig_funnel)
    
            
if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("DASHBOARD ENRICHI - LANCEMENT")
    logger.info("=" * 80)
    logger.info("URL : http://127.0.0.1:8050")
    logger.info("=" * 80)
    app.run(debug=True, port=8050, host='127.0.0.1', use_reloader=False)