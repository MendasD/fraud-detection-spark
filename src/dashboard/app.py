"""
Dashboard temps réel pour la détection de fraudes avec ML
Visualise les transactions et détections en direct depuis la table Spark en mémoire
"""

import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objs as go
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, window
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import sys

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# chemin python de l'environnement virtuel
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
os.environ["PYSPARK_DRIVER_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-21.0.6.7-hotspot" # Java 17 compatible pour spark 3.5.x

# Configurer HADOOP_HOME pour Windows si défini dans .env
hadoop_home = os.getenv('HADOOP_HOME')
if hadoop_home and os.path.exists(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
    bin_path = os.path.join(hadoop_home, 'bin')
    if bin_path not in os.environ.get('PATH', ''):
        os.environ['PATH'] = f"{bin_path};{os.environ.get('PATH', '')}"
    logger.info(f"HADOOP_HOME configuré: {hadoop_home}")


# Couleurs du thème
COLORS = {
    'background': '#0f1419',
    'card': '#1a1f2e',
    'text': '#ffffff',
    'primary': '#00d4ff',
    'danger': '#ff4757',
    'warning': '#ffa502',
    'success': '#2ed573',
    'safe': '#26de81'
}


class FraudDashboard:
    """Dashboard temps réel pour la détection de fraudes"""
    
    def __init__(self):
        """Initialise le dashboard"""
        logger.info("🚀 Initialisation du dashboard")
        
        # Connexion à Spark
        self.spark = self._get_spark_session()
        
        # Créer l'application Dash
        self.app = dash.Dash(__name__)
        self.app.title = "Fraud Detection - Real-Time Dashboard"
        
        # Layout
        self.app.layout = self._create_layout()
        
        # Callbacks
        self._setup_callbacks()
        
        logger.info("✅ Dashboard initialisé")
    
    def _get_spark_session(self):
        """Récupère ou crée la session Spark"""
        try:
            spark = SparkSession.builder \
                .appName("FraudDashboard") \
                .master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("ERROR")
            logger.info(f"✅ Connecté à Spark {spark.version}")
            return spark
            
        except Exception as e:
            logger.error(f"❌ Erreur Spark: {e}")
            raise
    
    def _create_layout(self):
        """Crée le layout du dashboard"""
        return html.Div([
            # En-tête
            html.Div([
                html.H1("🛡️ Fraud Detection - Real-Time ML Dashboard", 
                       style={'color': COLORS['primary'], 'textAlign': 'center'}),
                html.P("Détection de fraudes en temps réel avec Machine Learning",
                      style={'color': COLORS['text'], 'textAlign': 'center', 'fontSize': 18})
            ], style={'backgroundColor': COLORS['card'], 'padding': '20px', 'marginBottom': '20px'}),
            
            # Intervalle de mise à jour
            dcc.Interval(
                id='interval-component',
                interval=5*1000,  # 5 secondes
                n_intervals=0
            ),
            
            # Indicateurs clés (KPIs)
            html.Div(id='kpi-cards', children=[]),
            
            # Graphiques principaux
            html.Div([
                # Graphique 1: Transactions au fil du temps
                html.Div([
                    dcc.Graph(id='transactions-timeline')
                ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
                
                # Graphique 2: Distribution des scores de fraude
                html.Div([
                    dcc.Graph(id='fraud-score-distribution')
                ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            ]),
            
            # Deuxième ligne de graphiques
            html.Div([
                # Graphique 3: Fraudes par catégorie
                html.Div([
                    dcc.Graph(id='fraud-by-category')
                ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
                
                # Graphique 4: Carte géographique
                html.Div([
                    dcc.Graph(id='fraud-map')
                ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            ]),
            
            # Troisième ligne: Métriques ML
            html.Div([
                # Graphique 5: Performance du modèle
                html.Div([
                    dcc.Graph(id='model-performance')
                ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
                
                # Graphique 6: Top transactions suspectes
                html.Div([
                    html.Div(id='suspicious-transactions')
                ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            ]),
            
        ], style={'backgroundColor': COLORS['background'], 'padding': '20px', 'minHeight': '100vh'})
    
    def _get_data(self):
        """Récupère les données depuis la table Spark en mémoire"""
        try:
            # Lire depuis la table en mémoire
           # df = self.spark.sql("SELECT * FROM fraud_detection_ml")
            
            # Convertir en Pandas pour Plotly
            #pdf = df.toPandas()
            
            # if len(pdf) == 0:
            #     logger.warning("⚠️ Aucune donnée disponible")
            #     return None
            
            # Convertir timestamp
            #pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])

            df = self.spark.read.parquet("./data/transactions")
            df.createOrReplaceTempView("fraud_detection_ml")
            df = self.spark.sql("SELECT * FROM fraud_detection_ml")
            pdf = df.toPandas()
            if len(pdf) == 0:
                logger.warning("⚠️ Aucune donnée disponible")
                return None
            pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])
            
            logger.info(f" {len(pdf)} transactions récupérées")
            return pdf
            
        except Exception as e:
            logger.error(f" Erreur récupération données: {e}")
            return None
    
    def _setup_callbacks(self):
        """Configure les callbacks pour la mise à jour automatique"""
        
        @self.app.callback(
            [Output('kpi-cards', 'children'),
             Output('transactions-timeline', 'figure'),
             Output('fraud-score-distribution', 'figure'),
             Output('fraud-by-category', 'figure'),
             Output('fraud-map', 'figure'),
             Output('model-performance', 'figure'),
             Output('suspicious-transactions', 'children')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_dashboard(n):
            """Met à jour tous les composants du dashboard"""
            
            # Récupérer les données
            pdf = self._get_data()
            
            if pdf is None or len(pdf) == 0:
                # Retourner des graphiques vides
                empty_fig = go.Figure()
                empty_fig.update_layout(
                    template='plotly_dark',
                    paper_bgcolor=COLORS['card'],
                    plot_bgcolor=COLORS['card'],
                    font_color=COLORS['text']
                )
                
                no_data_msg = html.Div([
                    html.H3("⏳ En attente de données...", 
                           style={'color': COLORS['warning'], 'textAlign': 'center'}),
                    html.P("Vérifiez que le détecteur ML est lancé et que des transactions arrivent.",
                          style={'color': COLORS['text'], 'textAlign': 'center'})
                ], style={'padding': '50px'})
                
                return (no_data_msg, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, no_data_msg)
            
            # 1. KPI Cards
            kpi_cards = self._create_kpi_cards(pdf)
            
            # 2. Timeline des transactions
            timeline_fig = self._create_timeline(pdf)
            
            # 3. Distribution des scores
            score_dist_fig = self._create_score_distribution(pdf)
            
            # 4. Fraudes par catégorie
            category_fig = self._create_fraud_by_category(pdf)
            
            # 5. Carte géographique
            map_fig = self._create_fraud_map(pdf)
            
            # 6. Performance du modèle
            performance_fig = self._create_model_performance(pdf)
            
            # 7. Top transactions suspectes
            suspicious_table = self._create_suspicious_table(pdf)
            
            return (kpi_cards, timeline_fig, score_dist_fig, category_fig, 
                   map_fig, performance_fig, suspicious_table)
    
    def _create_kpi_cards(self, pdf):
        """Crée les cartes KPI"""
        total = len(pdf)
        frauds_detected = len(pdf[pdf['predicted_fraud'] == 1.0])
        fraud_rate = (frauds_detected / total * 100) if total > 0 else 0
        avg_score = pdf['fraud_score'].mean()
        total_amount = pdf['amount'].sum()
        fraud_amount = pdf[pdf['predicted_fraud'] == 1.0]['amount'].sum()
        
        # Calculer les métriques ML
        tp = len(pdf[(pdf['is_fraud'] == 1) & (pdf['predicted_fraud'] == 1.0)])
        fp = len(pdf[(pdf['is_fraud'] == 0) & (pdf['predicted_fraud'] == 1.0)])
        tn = len(pdf[(pdf['is_fraud'] == 0) & (pdf['predicted_fraud'] == 0.0)])
        fn = len(pdf[(pdf['is_fraud'] == 1) & (pdf['predicted_fraud'] == 0.0)])
        
        accuracy = ((tp + tn) / total * 100) if total > 0 else 0
        precision = (tp / (tp + fp) * 100) if (tp + fp) > 0 else 0
        recall = (tp / (tp + fn) * 100) if (tp + fn) > 0 else 0
        
        cards = html.Div([
            # Carte 1: Total transactions
            self._create_kpi_card("📊 Total Transactions", f"{total:,}", COLORS['primary']),
            
            # Carte 2: Fraudes détectées
            self._create_kpi_card("🚨 Fraudes Détectées", f"{frauds_detected:,} ({fraud_rate:.1f}%)", COLORS['danger']),
            
            # Carte 3: Score moyen
            self._create_kpi_card("📈 Score Fraude Moyen", f"{avg_score:.1f}/100", COLORS['warning']),
            
            # Carte 4: Montant total
            self._create_kpi_card("💰 Montant Total", f"${total_amount:,.2f}", COLORS['success']),
            
            # Carte 5: Montant frauduleux
            self._create_kpi_card("💸 Montant Frauduleux", f"${fraud_amount:,.2f}", COLORS['danger']),
            
            # Carte 6: Accuracy
            self._create_kpi_card("🎯 Précision Modèle", f"{accuracy:.1f}%", COLORS['success']),
            
            # Carte 7: Precision
            self._create_kpi_card("🔍 Précision ML", f"{precision:.1f}%", COLORS['primary']),
            
            # Carte 8: Recall
            self._create_kpi_card("📡 Rappel ML", f"{recall:.1f}%", COLORS['primary']),
            
        ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around', 'marginBottom': '20px'})
        
        return cards
    
    def _create_kpi_card(self, title, value, color):
        """Crée une carte KPI individuelle"""
        return html.Div([
            html.H4(title, style={'color': COLORS['text'], 'fontSize': 14, 'marginBottom': 10}),
            html.H2(value, style={'color': color, 'fontSize': 28, 'fontWeight': 'bold', 'margin': 0})
        ], style={
            'backgroundColor': COLORS['card'],
            'padding': '20px',
            'margin': '10px',
            'borderRadius': '10px',
            'minWidth': '200px',
            'textAlign': 'center',
            'boxShadow': '0 4px 6px rgba(0,0,0,0.3)'
        })
    
    def _create_timeline(self, pdf):
        """Graphique timeline des transactions"""
        # Grouper par minute
        pdf_sorted = pdf.sort_values('timestamp')
        pdf_sorted['minute'] = pdf_sorted['timestamp'].dt.floor('1min')
        
        timeline_data = pdf_sorted.groupby(['minute', 'predicted_fraud']).size().reset_index(name='count')
        
        fig = go.Figure()
        
        # Transactions légitimes
        legit = timeline_data[timeline_data['predicted_fraud'] == 0.0]
        fig.add_trace(go.Scatter(
            x=legit['minute'], y=legit['count'],
            mode='lines+markers',
            name='Légitimes',
            line=dict(color=COLORS['success'], width=2),
            marker=dict(size=6)
        ))
        
        # Transactions frauduleuses
        fraud = timeline_data[timeline_data['predicted_fraud'] == 1.0]
        fig.add_trace(go.Scatter(
            x=fraud['minute'], y=fraud['count'],
            mode='lines+markers',
            name='Frauduleuses',
            line=dict(color=COLORS['danger'], width=2),
            marker=dict(size=6)
        ))
        
        fig.update_layout(
            title='📊 Transactions au Fil du Temps',
            xaxis_title='Temps',
            yaxis_title='Nombre de transactions',
            template='plotly_dark',
            paper_bgcolor=COLORS['card'],
            plot_bgcolor=COLORS['card'],
            font_color=COLORS['text'],
            hovermode='x unified'
        )
        
        return fig
    
    def _create_score_distribution(self, pdf):
        """Distribution des scores de fraude"""
        fig = go.Figure()
        
        fig.add_trace(go.Histogram(
            x=pdf['fraud_score'],
            nbinsx=20,
            marker_color=COLORS['primary'],
            name='Distribution'
        ))
        
        # Lignes de seuil
        fig.add_vline(x=30, line_dash="dash", line_color=COLORS['safe'], 
                     annotation_text="LOW", annotation_position="top")
        fig.add_vline(x=50, line_dash="dash", line_color=COLORS['warning'], 
                     annotation_text="MEDIUM", annotation_position="top")
        fig.add_vline(x=70, line_dash="dash", line_color=COLORS['danger'], 
                     annotation_text="HIGH", annotation_position="top")
        
        fig.update_layout(
            title='📈 Distribution des Scores de Fraude',
            xaxis_title='Score de Fraude',
            yaxis_title='Nombre de transactions',
            template='plotly_dark',
            paper_bgcolor=COLORS['card'],
            plot_bgcolor=COLORS['card'],
            font_color=COLORS['text']
        )
        
        return fig
    
    def _create_fraud_by_category(self, pdf):
        """Fraudes par catégorie de marchand"""
        category_data = pdf.groupby(['merchant_category', 'predicted_fraud']).size().reset_index(name='count')
        
        fig = px.bar(
            category_data,
            x='merchant_category',
            y='count',
            color='predicted_fraud',
            barmode='group',
            color_discrete_map={0.0: COLORS['success'], 1.0: COLORS['danger']},
            labels={'predicted_fraud': 'Type', 'count': 'Nombre'}
        )
        
        fig.update_layout(
            title='🏪 Fraudes par Catégorie de Marchand',
            xaxis_title='Catégorie',
            yaxis_title='Nombre de transactions',
            template='plotly_dark',
            paper_bgcolor=COLORS['card'],
            plot_bgcolor=COLORS['card'],
            font_color=COLORS['text']
        )
        
        return fig
    
    def _create_fraud_map(self, pdf):
        """Carte géographique des fraudes"""
        fraud_data = pdf[pdf['predicted_fraud'] == 1.0]
        
        fig = px.scatter_mapbox(
            fraud_data,
            lat='location_lat',
            lon='location_lon',
            size='amount',
            color='fraud_score',
            color_continuous_scale='Reds',
            hover_data=['transaction_id', 'amount', 'fraud_score'],
            zoom=3,
            height=400
        )
        
        fig.update_layout(
            mapbox_style="carto-darkmatter",
            title='🗺️ Localisation des Fraudes Détectées',
            template='plotly_dark',
            paper_bgcolor=COLORS['card'],
            font_color=COLORS['text']
        )
        
        return fig
    
    def _create_model_performance(self, pdf):
        """Matrice de confusion et métriques"""
        tp = len(pdf[(pdf['is_fraud'] == 1) & (pdf['predicted_fraud'] == 1.0)])
        fp = len(pdf[(pdf['is_fraud'] == 0) & (pdf['predicted_fraud'] == 1.0)])
        tn = len(pdf[(pdf['is_fraud'] == 0) & (pdf['predicted_fraud'] == 0.0)])
        fn = len(pdf[(pdf['is_fraud'] == 1) & (pdf['predicted_fraud'] == 0.0)])
        
        # Matrice de confusion
        confusion_matrix = [[tn, fp], [fn, tp]]
        
        fig = go.Figure(data=go.Heatmap(
            z=confusion_matrix,
            x=['Prédit: Légit', 'Prédit: Fraude'],
            y=['Réel: Légit', 'Réel: Fraude'],
            colorscale='Blues',
            text=confusion_matrix,
            texttemplate='%{text}',
            textfont={"size": 20},
            showscale=False
        ))
        
        fig.update_layout(
            title='🎯 Matrice de Confusion du Modèle ML',
            template='plotly_dark',
            paper_bgcolor=COLORS['card'],
            plot_bgcolor=COLORS['card'],
            font_color=COLORS['text'],
            xaxis={'side': 'top'}
        )
        
        return fig
    
    def _create_suspicious_table(self, pdf):
        """Table des transactions les plus suspectes"""
        top_suspicious = pdf.nlargest(10, 'fraud_score')[
            ['transaction_id', 'amount', 'merchant_category', 'fraud_score', 'risk_level', 'predicted_fraud']
        ]
        
        rows = []
        for _, row in top_suspicious.iterrows():
            risk_color = {
                'HIGH': COLORS['danger'],
                'MEDIUM': COLORS['warning'],
                'LOW': COLORS['safe'],
                'SAFE': COLORS['success']
            }.get(row['risk_level'], COLORS['text'])
            
            rows.append(html.Tr([
                html.Td(row['transaction_id'][:10] + '...', style={'color': COLORS['text']}),
                html.Td(f"${row['amount']:.2f}", style={'color': COLORS['text']}),
                html.Td(row['merchant_category'], style={'color': COLORS['text']}),
                html.Td(f"{row['fraud_score']:.1f}", style={'color': COLORS['text']}),
                html.Td(row['risk_level'], style={'color': risk_color, 'fontWeight': 'bold'}),
                html.Td('🚨' if row['predicted_fraud'] == 1.0 else '✅', 
                       style={'fontSize': 20, 'textAlign': 'center'})
            ]))
        
        table = html.Div([
            html.H3("🔍 Top 10 Transactions Suspectes", 
                   style={'color': COLORS['primary'], 'marginBottom': 15}),
            html.Table([
                html.Thead(html.Tr([
                    html.Th('Transaction ID', style={'color': COLORS['primary']}),
                    html.Th('Montant', style={'color': COLORS['primary']}),
                    html.Th('Catégorie', style={'color': COLORS['primary']}),
                    html.Th('Score', style={'color': COLORS['primary']}),
                    html.Th('Risque', style={'color': COLORS['primary']}),
                    html.Th('Statut', style={'color': COLORS['primary']})
                ])),
                html.Tbody(rows)
            ], style={
                'width': '100%',
                'backgroundColor': COLORS['card'],
                'borderCollapse': 'collapse'
            })
        ], style={
            'backgroundColor': COLORS['card'],
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 4px 6px rgba(0,0,0,0.3)'
        })
        
        return table
    
    def run(self, debug=True, port=8050):
        """Lance le dashboard"""
        logger.info(f"🚀 Démarrage du dashboard sur http://localhost:{port}")
        self.app.run(debug=debug, port=port, host='0.0.0.0')


if __name__ == "__main__":
    dashboard = FraudDashboard()
    dashboard.run(debug=True, port=8050)