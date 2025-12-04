"""
Script simple pour lancer l'entraînement du modèle ML.
Usage: python run_train_model.py
"""

from src.models.train_model import FraudModelTrainer

if __name__ == "__main__":
    print("="*60)
    print("🤖 ENTRAÎNEMENT DU MODÈLE ML - DÉTECTION DE FRAUDES")
    print("="*60)
    print("\n📊 Configuration:")
    print("  • Algorithme: Random Forest")
    print("  • Arbres: 100")
    print("  • Profondeur max: 10")
    print("  • Données: 13M transactions")
    print("\n⏳ Temps estimé: 5-15 minutes")
    print("\n💡 Le modèle sera sauvegardé dans: data/models/")
    
    input("\n⏸️  Appuyez sur Entrée pour démarrer l'entraînement...")
    
    # Lancer l'entraînement
    trainer = FraudModelTrainer(data_path="../../data/historical")
    model, metrics = trainer.run()
    
    print("\n" + "="*60)
    print("✅ MODÈLE ENTRAÎNÉ ET SAUVEGARDÉ !")
    print("="*60)
    print(f"\n📊 Performance:")
    print(f"  • Accuracy: {metrics['accuracy']*100:.2f}%")
    print(f"  • F1-Score: {metrics['f1_score']*100:.2f}%")
    print(f"  • AUC-ROC: {metrics['auc']:.4f}")
    print("\n🎯 Prochaine étape:")
    print("  1. Intégrer le modèle au streaming: run_detector_ml.py")
    print("  2. Lancer le dashboard: run_dashboard.py")
    print("="*60)