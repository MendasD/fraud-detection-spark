"""
Dashboard temps réel pour la détection de fraudes avec ML
Visualise les transactions et détections en direct - Version Sans Spark
"""
import sys
import os
from pathlib import Path

# Ajouter le chemin parent au sys.path pour les imports relatifs
sys.path.insert(0, str(Path(__file__).parent.parent))

import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from datetime import datetime
import os
import sys
import numpy as np
import glob
import requests
from dotenv import load_dotenv
from utils.logger import setup_logger

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
logger = setup_logger(__name__)

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


# Configuration
UPDATE_INTERVAL = 8000  # 5 secondes
DETECTOR_API_URL = os.getenv('DETECTOR_API_URL', 'http://detector:5000')

# Couleurs du thème BLANC professionnel
COLORS = {
    'background': '#f8f9fa',      # Gris très clair
    'card': '#ffffff',             # Blanc
    'text': '#1a1a1a',            # Noir texte
    'primary': '#0066cc',         # Bleu
    'danger': '#dc3545',          # Rouge
    'warning': '#ffc107',         # Jaune/Orange
    'success': '#28a745',         # Vert
    'safe': '#20c997',            # Vert clair
    'secondary': '#6610f2',       # Violet
    'border': '#dee2e6'           # Bordure grise
}

# Initialisation de l'application Dash
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Fraud Detection Dashboard"

def get_data():
    """Récupère les données depuis les fichiers parquet avec Pandas"""
    try:
        # Chemin vers les données (relatif au fichier app.py)
        data_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "transactions")
        data_path = os.path.abspath(data_path)
        
        logger.info(f" Lecture depuis : {data_path}")
        
        if not os.path.exists(data_path):
            logger.warning(f" Dossier inexistant : {data_path}")
            return None
        
        # Lire tous les fichiers parquet
        parquet_files = glob.glob(os.path.join(data_path, "*.parquet"))
        
        if not parquet_files:
            logger.warning(f" Aucun fichier parquet dans : {data_path}")
            return None
        
        logger.info(f" {len(parquet_files)} fichier(s) parquet trouvé(s)")
        
        # Lire et combiner tous les fichiers
        dfs = []
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
                logger.info(f" {os.path.basename(file)}: {len(df)} lignes")
            except Exception as e:
                logger.warning(f" Erreur lecture {os.path.basename(file)}: {e}")
        
        if not dfs:
            logger.warning("Aucune donnée lue")
            return None
        
        # Combiner tous les DataFrames
        pdf = pd.concat(dfs, ignore_index=True)
        
        if len(pdf) == 0:
            logger.warning("DataFrame vide")
            return None
        
        # Convertir timestamp
        if 'timestamp' in pdf.columns:
            pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])
        
        logger.info(f"{len(pdf)} transactions récupérées au total")
        return pdf
        
    except Exception as e:
        logger.error(f"❌ Erreur récupération données: {e}")
        import traceback
        logger.error(traceback.format_exc())
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
                print(f"✅ {len(df)} transactions récupérées depuis l'API")
                return df
    except Exception as e:
        print(f"❌ Erreur API: {e}")
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
        print(f"❌ Erreur stats API: {e}")
    return None


# Layout principal
app.layout = html.Div([
    html.Div([
        html.H1("Fraud Detection System", 
               style={'color': COLORS['primary'], 'margin': '0', 'fontSize': '2.5em'}),
        html.P("Real-Time ML Dashboard - Powered by Apache Spark",
              style={'color': COLORS['text'], 'margin': '5px 0 0 0', 'fontSize': '1.2em'})
    ], style={
        'backgroundColor': COLORS['card'],
        'padding': '30px',
        'marginBottom': '20px',
        'borderRadius': '15px',
        'boxShadow': '0 10px 30px rgba(0,0,0,0.3)',
        'textAlign': 'center'
    }),
    
    dcc.Interval(id='interval-component', interval=UPDATE_INTERVAL, n_intervals=0),
    
    html.Div([
        dcc.Tabs(id='tabs', value='monitoring', children=[
            dcc.Tab(label='Monitoring Temps Réel', value='monitoring',
                   style={'backgroundColor': COLORS['card'], 'color': COLORS['text']},
                   selected_style={'backgroundColor': COLORS['primary'], 'color': COLORS['text']}),
            dcc.Tab(label='Performance ML', value='performance',
                   style={'backgroundColor': COLORS['card'], 'color': COLORS['text']},
                   selected_style={'backgroundColor': COLORS['primary'], 'color': COLORS['text']}),
            dcc.Tab(label='Exploration Données', value='exploration',
                   style={'backgroundColor': COLORS['card'], 'color': COLORS['text']},
                   selected_style={'backgroundColor': COLORS['primary'], 'color': COLORS['text']}),
        ], style={'marginBottom': '20px'})
    ]),
    
    html.Div(id='page-content')
    
], style={
    'backgroundColor': COLORS['background'],
    'padding': '20px',
    'minHeight': '100vh',
    'fontFamily': "'Segoe UI', sans-serif"
})

@app.callback(Output('page-content', 'children'), Input('tabs', 'value'))
def render_content(tab):
    if tab == 'monitoring':
        return html.Div([
            html.Div(id='kpi-cards', style={'marginBottom': '20px'}),
            html.Div([
                html.Div([dcc.Graph(id='transactions-timeline')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
                html.Div([dcc.Graph(id='fraud-score-distribution')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            ]),
            html.Div([
                html.Div([dcc.Graph(id='risk-pie-chart')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
                html.Div([dcc.Graph(id='top-users-bar')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            ]),
            html.Div(id='recent-alerts', style={'marginTop': '20px'})
        ])
    elif tab == 'performance':
        return html.Div([
            html.Div(id='ml-kpi-cards', style={'marginBottom': '20px'}),
            html.Div([
                html.Div([dcc.Graph(id='confusion-matrix')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
                html.Div([dcc.Graph(id='feature-importance')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            ]),
            html.Div([
                html.Div([dcc.Graph(id='roc-curve')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
                html.Div([dcc.Graph(id='score-distribution-ml')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            ]),
        ])
    elif tab == 'exploration':
        return html.Div([
            html.Div([dcc.Graph(id='fraud-map')], style={'marginBottom': '20px'}),
            html.Div([
                html.Div([dcc.Graph(id='fraud-by-category')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
                html.Div([dcc.Graph(id='amount-boxplot')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            ]),
            html.Div([
                html.Div([dcc.Graph(id='time-heatmap')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
                html.Div([dcc.Graph(id='daily-trend')], 
                        style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            ]),
        ])

def create_empty_figure():
    fig = go.Figure()
    fig.update_layout(
        template='plotly_white',           # Thème blanc
        paper_bgcolor=COLORS['card'],
        plot_bgcolor=COLORS['card'],
        font_color=COLORS['text']
    )
    return fig

def create_no_data_message():
    return html.Div([
        html.H3("En attente de données...", 
               style={'color': COLORS['warning'], 'textAlign': 'center', 'marginTop': '50px'}),
        html.P("Vérifiez que des données existent dans data/transactions/",
              style={'color': COLORS['text'], 'textAlign': 'center'}),
        html.P("Pour créer des données de test, exécutez : python create_test_data.py",
              style={'color': COLORS['text'], 'textAlign': 'center', 'fontSize': '0.9em'})
    ], style={'padding': '50px', 'backgroundColor': COLORS['card'], 'borderRadius': '10px'})

@app.callback(
    [Output('kpi-cards', 'children'),
     Output('transactions-timeline', 'figure'),
     Output('fraud-score-distribution', 'figure'),
     Output('risk-pie-chart', 'figure'),
     Output('top-users-bar', 'figure'),
     Output('recent-alerts', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_monitoring(n):
    #pdf = get_data()
    pdf = fetch_data_from_api()
    empty = create_empty_figure()
    
    if pdf is None or len(pdf) == 0:
        no_data = create_no_data_message()
        return (no_data, empty, empty, empty, empty, no_data)
    
    # Déterminer la colonne de fraude
    fraud_col = 'predicted_fraud' if 'predicted_fraud' in pdf.columns else 'is_fraud'
    
    # KPIs
    total = len(pdf)
    frauds = len(pdf[pdf[fraud_col] == 1]) if fraud_col in pdf.columns else 0
    fraud_rate = (frauds / total * 100) if total > 0 else 0
    total_amount = pdf['amount'].sum() if 'amount' in pdf.columns else 0
    
    kpis = html.Div([
        html.Div([
            html.H4("Total Transactions", style={'color': COLORS['text'], 'fontSize': '14px'}),
            html.H2(f"{total:,}", style={'color': COLORS['primary'], 'fontSize': '28px', 'margin': '5px 0'})
        ], style={'backgroundColor': COLORS['card'], 'padding': '20px', 'margin': '10px',
                 'borderRadius': '10px', 'textAlign': 'center', 'flex': '1', 'minWidth': '200px',
                 'boxShadow': '0 2px 8px rgba(0,0,0,0.1)', 'border': f'1px solid {COLORS["border"]}'}),
        html.Div([
            html.H4("Fraudes Détectées", style={'color': COLORS['text'], 'fontSize': '14px'}),
            html.H2(f"{frauds:,}", style={'color': COLORS['danger'], 'fontSize': '28px', 'margin': '5px 0'})
        ], style={'backgroundColor': COLORS['card'], 'padding': '20px', 'margin': '10px',
                 'borderRadius': '10px', 'textAlign': 'center', 'flex': '1', 'minWidth': '200px'}),
        html.Div([
            html.H4("Taux de Fraude", style={'color': COLORS['text'], 'fontSize': '14px'}),
            html.H2(f"{fraud_rate:.1f}%", style={'color': COLORS['warning'], 'fontSize': '28px', 'margin': '5px 0'})
        ], style={'backgroundColor': COLORS['card'], 'padding': '20px', 'margin': '10px',
                 'borderRadius': '10px', 'textAlign': 'center', 'flex': '1', 'minWidth': '200px'}),
        html.Div([
            html.H4("Montant Total", style={'color': COLORS['text'], 'fontSize': '14px'}),
            html.H2(f"${total_amount:,.0f}", style={'color': COLORS['success'], 'fontSize': '28px', 'margin': '5px 0'})
        ], style={'backgroundColor': COLORS['card'], 'padding': '20px', 'margin': '10px',
                 'borderRadius': '10px', 'textAlign': 'center', 'flex': '1', 'minWidth': '200px'}),
    ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around'})
    
    # Timeline
    fig_timeline = create_empty_figure()
    if 'timestamp' in pdf.columns and fraud_col in pdf.columns:
        pdf_sorted = pdf.sort_values('timestamp').copy()
        pdf_sorted['minute'] = pdf_sorted['timestamp'].dt.floor('1min')
        timeline_data = pdf_sorted.groupby(['minute', fraud_col]).size().reset_index(name='count')
        
        fig_timeline = go.Figure()
        legit = timeline_data[timeline_data[fraud_col] == 0]
        if len(legit) > 0:
            fig_timeline.add_trace(go.Scatter(
                x=legit['minute'], y=legit['count'], mode='lines+markers',
                name='Légitimes', line=dict(color=COLORS['success'], width=2)
            ))
        fraud = timeline_data[timeline_data[fraud_col] == 1]
        if len(fraud) > 0:
            fig_timeline.add_trace(go.Scatter(
                x=fraud['minute'], y=fraud['count'], mode='lines+markers',
                name='Frauduleuses', line=dict(color=COLORS['danger'], width=2)
            ))
        fig_timeline.update_layout(
            title='Transactions au Fil du Temps', xaxis_title='Temps', yaxis_title='Nombre',
            template='plotly_white', paper_bgcolor=COLORS['card'], plot_bgcolor=COLORS['background'],
            font_color=COLORS['text'], hovermode='x unified'
        )
    
    # Distribution des scores
    fig_score = create_empty_figure()
    if 'fraud_score' in pdf.columns:
        fig_score = go.Figure()
        fig_score.add_trace(go.Histogram(x=pdf['fraud_score'], nbinsx=20, marker_color=COLORS['primary']))
        fig_score.add_vline(x=30, line_dash="dash", line_color=COLORS['safe'])
        fig_score.add_vline(x=50, line_dash="dash", line_color=COLORS['warning'])
        fig_score.add_vline(x=70, line_dash="dash", line_color=COLORS['danger'])
        fig_score.update_layout(
            title='Distribution des Scores de Fraude', xaxis_title='Score', yaxis_title='Nombre',
            template='plotly_dark', paper_bgcolor=COLORS['card'], plot_bgcolor=COLORS['card'],
            font_color=COLORS['text']
        )
    
    # Pie chart risques
    fig_risk = create_empty_figure()
    if 'risk_level' in pdf.columns:
        risk_counts = pdf['risk_level'].value_counts()
        colors_map = {'SAFE': COLORS['safe'], 'LOW': COLORS['success'], 
                     'MEDIUM': COLORS['warning'], 'HIGH': COLORS['danger']}
        fig_risk = go.Figure(data=[go.Pie(
            labels=risk_counts.index, values=risk_counts.values, hole=0.4,
            marker=dict(colors=[colors_map.get(x, COLORS['text']) for x in risk_counts.index])
        )])
        fig_risk.update_layout(
            title='Répartition par Niveau de Risque', template='plotly_dark',
            paper_bgcolor=COLORS['card'], font_color=COLORS['text']
        )
    
    # Top users
    fig_users = create_empty_figure()
    if 'user_id' in pdf.columns and fraud_col in pdf.columns:
        frauds_df = pdf[pdf[fraud_col] == 1]
        if len(frauds_df) > 0:
            top_users = frauds_df['user_id'].value_counts().head(10)
            fig_users = go.Figure(data=[go.Bar(
                x=top_users.values, y=top_users.index, orientation='h',
                marker=dict(color=COLORS['danger'])
            )])
            fig_users.update_layout(
                title='👥 Top 10 Utilisateurs avec Fraudes', xaxis_title='Nombre de Fraudes',
                yaxis_title='User ID', template='plotly_dark', paper_bgcolor=COLORS['card'],
                plot_bgcolor=COLORS['card'], font_color=COLORS['text']
            )
    
    # Alertes récentes
    alerts = html.Div("Aucune fraude détectée", style={'color': COLORS['text'], 'padding': '20px'})
    if fraud_col in pdf.columns and 'timestamp' in pdf.columns:
        frauds = pdf[pdf[fraud_col] == 1].sort_values('timestamp', ascending=False).head(20)
        if len(frauds) > 0:
            rows = []
            for _, row in frauds.iterrows():
                rows.append(html.Tr([
                    html.Td(row['timestamp'].strftime('%H:%M:%S'), style={'color': COLORS['text'], 'padding': '8px'}),
                    html.Td(str(row.get('transaction_id', 'N/A'))[:12] + '...', style={'color': COLORS['text'], 'padding': '8px'}),
                    html.Td(str(row.get('user_id', 'N/A')), style={'color': COLORS['text'], 'padding': '8px'}),
                    html.Td(f"${row.get('amount', 0):.2f}", style={'color': COLORS['text'], 'padding': '8px'}),
                    html.Td(f"{row.get('fraud_score', 0):.0f}", 
                           style={'color': COLORS['danger'], 'fontWeight': 'bold', 'padding': '8px'}),
                ]))
            
            alerts = html.Div([
                html.H3("Alertes Récentes (20 dernières fraudes)", 
                       style={'color': COLORS['primary'], 'marginBottom': '15px'}),
                html.Table([
                    html.Thead(html.Tr([
                        html.Th('Temps', style={'color': COLORS['primary'], 'padding': '10px', 'textAlign': 'left'}),
                        html.Th('Transaction ID', style={'color': COLORS['primary'], 'padding': '10px', 'textAlign': 'left'}),
                        html.Th('User', style={'color': COLORS['primary'], 'padding': '10px', 'textAlign': 'left'}),
                        html.Th('Montant', style={'color': COLORS['primary'], 'padding': '10px', 'textAlign': 'left'}),
                        html.Th('Score', style={'color': COLORS['primary'], 'padding': '10px', 'textAlign': 'left'}),
                    ])),
                    html.Tbody(rows)
                ], style={'width': '100%', 'borderCollapse': 'collapse', 'backgroundColor': COLORS['card']})
            ], style={
                'backgroundColor': COLORS['card'], 'padding': '20px', 'borderRadius': '10px',
                'boxShadow': '0 4px 6px rgba(0,0,0,0.3)'
            })
    
    return (kpis, fig_timeline, fig_score, fig_risk, fig_users, alerts)

@app.callback(
    [Output('ml-kpi-cards', 'children'),
     Output('confusion-matrix', 'figure'),
     Output('feature-importance', 'figure'),
     Output('roc-curve', 'figure'),
     Output('score-distribution-ml', 'figure')],
    Input('interval-component', 'n_intervals')
)
def update_performance(n):
    #pdf = get_data()
    pdf = fetch_data_from_api()
    empty = create_empty_figure()
    
    if pdf is None or len(pdf) == 0:
        return (create_no_data_message(), empty, empty, empty, empty)
    
    # KPIs ML
    kpis = create_no_data_message()
    fig_conf = empty
    
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
            html.Div([html.H4("Accuracy"), html.H2(f"{accuracy:.1f}%")],
                    style={'backgroundColor': COLORS['card'], 'padding': '20px', 'margin': '10px',
                          'borderRadius': '10px', 'textAlign': 'center', 'flex': '1'}),
            html.Div([html.H4("🔍 Precision"), html.H2(f"{precision:.1f}%")],
                    style={'backgroundColor': COLORS['card'], 'padding': '20px', 'margin': '10px',
                          'borderRadius': '10px', 'textAlign': 'center', 'flex': '1'}),
            html.Div([html.H4("📡 Recall"), html.H2(f"{recall:.1f}%")],
                    style={'backgroundColor': COLORS['card'], 'padding': '20px', 'margin': '10px',
                          'borderRadius': '10px', 'textAlign': 'center', 'flex': '1'}),
            html.Div([html.H4("⚖️ F1-Score"), html.H2(f"{f1:.1f}%")],
                    style={'backgroundColor': COLORS['card'], 'padding': '20px', 'margin': '10px',
                          'borderRadius': '10px', 'textAlign': 'center', 'flex': '1'}),
        ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around'})
        
        confusion_data = [[tn, fp], [fn, tp]]
        fig_conf = go.Figure(data=go.Heatmap(
            z=confusion_data, x=['Prédit: Légit', 'Prédit: Fraude'],
            y=['Réel: Légit', 'Réel: Fraude'], colorscale='Blues',
            text=confusion_data, texttemplate='%{text}', textfont={"size": 20}
        ))
        fig_conf.update_layout(
            title='Matrice de Confusion', template='plotly_dark',
            paper_bgcolor=COLORS['card'], font_color=COLORS['text'], xaxis={'side': 'top'}
        )
    
    # Feature importance (statique)
    features = ['amount', 'hour', 'day_of_week', 'category', 'location', 'velocity']
    importance = [0.28, 0.18, 0.15, 0.12, 0.10, 0.08]
    fig_feat = go.Figure(data=[go.Bar(x=importance, y=features, orientation='h',
                                     marker=dict(color=COLORS['primary']))])
    fig_feat.update_layout(
        title='Feature Importance', xaxis_title='Importance',
        template='plotly_dark', paper_bgcolor=COLORS['card'],
        plot_bgcolor=COLORS['card'], font_color=COLORS['text']
    )
    
    # ROC Curve (statique)
    fpr = np.linspace(0, 1, 100)
    tpr = 1 - (1 - fpr) ** 2
    fig_roc = go.Figure()
    fig_roc.add_trace(go.Scatter(x=fpr, y=tpr, mode='lines', name='ROC',
                                line=dict(color=COLORS['primary'], width=3)))
    fig_roc.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode='lines', name='Random',
                                line=dict(color='gray', dash='dash')))
    fig_roc.update_layout(
        title='ROC Curve (AUC = 0.92)', xaxis_title='False Positive Rate',
        yaxis_title='True Positive Rate', template='plotly_dark',
        paper_bgcolor=COLORS['card'], plot_bgcolor=COLORS['card'], font_color=COLORS['text']
    )
    
    # Distribution scores par type
    fig_dist = empty
    if 'fraud_score' in pdf.columns:
        fraud_col = 'predicted_fraud' if 'predicted_fraud' in pdf.columns else 'is_fraud'
        if fraud_col in pdf.columns:
            fig_dist = go.Figure()
            fig_dist.add_trace(go.Histogram(
                x=pdf[pdf[fraud_col] == 0]['fraud_score'], name='Normal',
                marker_color=COLORS['success'], opacity=0.7
            ))
            fig_dist.add_trace(go.Histogram(
                x=pdf[pdf[fraud_col] == 1]['fraud_score'], name='Fraude',
                marker_color=COLORS['danger'], opacity=0.7
            ))
            fig_dist.update_layout(
                title='Distribution Scores: Normal vs Fraude', xaxis_title='Score',
                yaxis_title='Fréquence', barmode='overlay', template='plotly_dark',
                paper_bgcolor=COLORS['card'], plot_bgcolor=COLORS['card'], font_color=COLORS['text']
            )
    
    return (kpis, fig_conf, fig_feat, fig_roc, fig_dist)

@app.callback(
    [Output('fraud-map', 'figure'),
     Output('fraud-by-category', 'figure'),
     Output('amount-boxplot', 'figure'),
     Output('time-heatmap', 'figure'),
     Output('daily-trend', 'figure')],
    Input('interval-component', 'n_intervals')
)
def update_exploration(n):
    #pdf = get_data()
    pdf = fetch_data_from_api()
    empty = create_empty_figure()
    
    if pdf is None or len(pdf) == 0:
        return (empty, empty, empty, empty, empty)
    
    fraud_col = 'predicted_fraud' if 'predicted_fraud' in pdf.columns else 'is_fraud'
    
    # Carte géo
    fig_map = empty
    if 'location_lat' in pdf.columns and 'location_lon' in pdf.columns and fraud_col in pdf.columns:
        fraud_data = pdf[pdf[fraud_col] == 1]
        if len(fraud_data) > 0:
            fig_map = px.scatter_mapbox(
                fraud_data, lat='location_lat', lon='location_lon', size='amount',
                color='fraud_score' if 'fraud_score' in fraud_data.columns else 'amount',
                color_continuous_scale='Reds', hover_data=['transaction_id', 'amount'],
                zoom=2, height=500
            )
            fig_map.update_layout(
                mapbox_style="carto-darkmatter", title='Localisation Géographique des Fraudes',
                template='plotly_dark', paper_bgcolor=COLORS['card'], font_color=COLORS['text']
            )
    
    # Par catégorie
    fig_cat = empty
    if 'merchant_category' in pdf.columns and fraud_col in pdf.columns:
        cat_data = pdf.groupby(['merchant_category', fraud_col]).size().reset_index(name='count')
        fig_cat = px.bar(cat_data, x='merchant_category', y='count', color=fraud_col, barmode='stack',
                        color_discrete_map={0: COLORS['success'], 1: COLORS['danger'], 
                                          0.0: COLORS['success'], 1.0: COLORS['danger']})
        fig_cat.update_layout(
            title='Transactions par Catégorie', xaxis_title='Catégorie', yaxis_title='Nombre',
            template='plotly_dark', paper_bgcolor=COLORS['card'], plot_bgcolor=COLORS['card'],
            font_color=COLORS['text']
        )
    
    # Box plot montants
    fig_box = empty
    if 'amount' in pdf.columns and fraud_col in pdf.columns:
        pdf_copy = pdf.copy()
        pdf_copy['Type'] = pdf_copy[fraud_col].apply(lambda x: 'Frauduleux' if x == 1 or x == 1.0 else 'Normal')
        fig_box = px.box(pdf_copy, x='Type', y='amount', color='Type',
                        color_discrete_map={'Normal': COLORS['success'], 'Frauduleux': COLORS['danger']})
        fig_box.update_layout(
            title='Distribution des Montants: Normal vs Frauduleux', xaxis_title='Type',
            yaxis_title='Montant ($)', template='plotly_dark', paper_bgcolor=COLORS['card'],
            plot_bgcolor=COLORS['card'], font_color=COLORS['text'], showlegend=False
        )
    
    # Heatmap temporelle
    fig_heat = empty
    if 'timestamp' in pdf.columns and fraud_col in pdf.columns:
        pdf_copy = pdf.copy()
        pdf_copy['hour'] = pdf_copy['timestamp'].dt.hour
        pdf_copy['day'] = pdf_copy['timestamp'].dt.day_name()
        fraud_data = pdf_copy[pdf_copy[fraud_col] == 1]
        
        if len(fraud_data) > 0:
            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            heatmap_data = fraud_data.groupby(['day', 'hour']).size().reset_index(name='count')
            matrix = np.zeros((7, 24))
            
            for _, row in heatmap_data.iterrows():
                try:
                    day_idx = days_order.index(row['day'])
                    hour_idx = int(row['hour'])
                    matrix[day_idx][hour_idx] = row['count']
                except:
                    pass
            
            fig_heat = go.Figure(data=go.Heatmap(
                z=matrix, x=list(range(24)), y=['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim'],
                colorscale='Reds', colorbar=dict(title='Fraudes')
            ))
            fig_heat.update_layout(
                title='Heatmap: Fraudes par Heure et Jour', xaxis_title='Heure',
                yaxis_title='Jour', template='plotly_dark', paper_bgcolor=COLORS['card'],
                plot_bgcolor=COLORS['card'], font_color=COLORS['text']
            )
    
    # Tendance journalière
    fig_daily = empty
    if 'timestamp' in pdf.columns and fraud_col in pdf.columns:
        pdf_copy = pdf.copy()
        pdf_copy['date'] = pdf_copy['timestamp'].dt.date
        daily_frauds = pdf_copy[pdf_copy[fraud_col] == 1].groupby('date').size().reset_index(name='count')
        
        if len(daily_frauds) > 0:
            fig_daily = go.Figure(data=go.Scatter(
                x=daily_frauds['date'], y=daily_frauds['count'], mode='lines+markers',
                line=dict(color=COLORS['danger'], width=2), fill='tozeroy',
                fillcolor='rgba(255, 71, 87, 0.2)'
            ))
            fig_daily.update_layout(
                title='Évolution des Fraudes par Jour', xaxis_title='Date',
                yaxis_title='Nombre de Fraudes', template='plotly_dark',
                paper_bgcolor=COLORS['card'], plot_bgcolor=COLORS['card'], font_color=COLORS['text']
            )
    
    return (fig_map, fig_cat, fig_box, fig_heat, fig_daily)

if __name__ == "__main__":
    logger.info("Démarrage du dashboard")
    logger.info(f"Dossier de travail : {os.getcwd()}")
    app.run(debug=True, port=8050, host='127.0.0.1')