import os, re, time
import pandas as pd, numpy as np
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.naive_bayes import MultinomialNB, ComplementNB
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, f1_score,
    classification_report, confusion_matrix, ConfusionMatrixDisplay
)
import mlflow, mlflow.sklearn

def clean_vietnamese_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r'<[^>]*>', ' ', text)
    text = re.sub(r'[^\w\s\!\?\,\.\']', '', text, flags=re.UNICODE)
    return re.sub(r'\s+', ' ', text).strip()

def main():
    data_path = 'data/ev_reviews_vietnam_1529_cleaned.csv'
    artifacts_dir = 'docs/benchmark_artifacts'
    os.makedirs(artifacts_dir, exist_ok=True)
    
    print("====================================================================")
    print("🚗 STANDALONE EV SENTIMENT BENCHMARK (1,529 CLEANED REVIEWS)")
    print("====================================================================")
    df = pd.read_csv(data_path)
    print(f"Total reviews: {len(df)}")
    
    df['cleaned_text'] = df['text'].apply(clean_vietnamese_text)
    
    train_df, temp_df = train_test_split(df, test_size=0.20, random_state=42, stratify=df['sentiment'])
    val_df, test_df = train_test_split(temp_df, test_size=0.50, random_state=42, stratify=temp_df['sentiment'])
    train_val_df = pd.concat([train_df, val_df], ignore_index=True)
    
    print(f"Splits: Train={len(train_df)}, Val={len(val_df)}, Test={len(test_df)}")
    
    candidates = [
        {"key": "multinomial_nb", "name": "Multinomial Naive Bayes", "clf": MultinomialNB(alpha=1.0)},
        {"key": "complement_nb", "name": "Complement Naive Bayes", "clf": ComplementNB(alpha=1.0)},
        {"key": "logistic_regression", "name": "Logistic Regression", "clf": LogisticRegression(C=2.0, max_iter=1000, random_state=42)},
        {"key": "linear_svm", "name": "Linear SVM (SGD log_loss)", "clf": SGDClassifier(loss='log_loss', alpha=1e-4, max_iter=1000, random_state=42)},
        {"key": "random_forest", "name": "Random Forest", "clf": RandomForestClassifier(n_estimators=150, max_depth=15, random_state=42, n_jobs=-1)}
    ]
    
    experiment_name = "ev-sentiment-benchmark-standalone"
    mlflow.set_tracking_uri("sqlite:///data/mlflow.db")
    mlflow.set_experiment(experiment_name)
    
    results = []
    classes = ['negative', 'neutral', 'positive']
    
    for cand in candidates:
        name, key, clf = cand["name"], cand["key"], cand["clf"]
        print(f"\nEvaluating {name}...")
        
        pipe = Pipeline([
            ('tfidf', TfidfVectorizer(ngram_range=(1, 2), max_features=3000, min_df=2)),
            ('clf', clf)
        ])
        
        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        cv_scores = cross_val_score(pipe, train_val_df['cleaned_text'], train_val_df['sentiment'], cv=skf, scoring='f1_macro', n_jobs=-1)
        cv_mean, cv_std = float(np.mean(cv_scores)), float(np.std(cv_scores))
        
        t0 = time.time()
        pipe.fit(train_df['cleaned_text'], train_df['sentiment'])
        train_time = time.time() - t0
        
        y_val_pred = pipe.predict(val_df['cleaned_text'])
        y_val_true = val_df['sentiment']
        
        val_acc = accuracy_score(y_val_true, y_val_pred)
        val_f1 = f1_score(y_val_true, y_val_pred, average='macro', zero_division=0)
        p_class = f1_score(y_val_true, y_val_pred, average=None, labels=classes, zero_division=0)
        
        print(f" -> Val Macro-F1: {val_f1:.4f} | 5-Fold CV: {cv_mean:.4f} (+/- {cv_std:.4f}) | Acc: {val_acc:.4f} | Time: {train_time:.3f}s")
        
        cm = confusion_matrix(y_val_true, y_val_pred, labels=classes)
        fig, ax = plt.subplots(figsize=(5, 4))
        ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=classes).plot(ax=ax, cmap='Blues', values_format='d')
        plt.title(f"{name} (Val F1: {val_f1:.4f})")
        cm_path = os.path.join(artifacts_dir, f"{key}_cm.png")
        plt.tight_layout()
        plt.savefig(cm_path, dpi=120)
        plt.close()
        
        with mlflow.start_run(run_name=name):
            mlflow.log_metric("val_macro_f1", val_f1)
            mlflow.log_metric("val_accuracy", val_acc)
            mlflow.log_metric("cv_macro_f1_mean", cv_mean)
            mlflow.log_metric("cv_macro_f1_std", cv_std)
            mlflow.log_metric("train_time_sec", train_time)
            mlflow.log_artifact(cm_path)
            mlflow.sklearn.log_model(pipe, artifact_path="model", serialization_format="pickle")
            
        results.append({
            "name": name, "val_f1": val_f1, "val_acc": val_acc,
            "cv_mean": cv_mean, "cv_std": cv_std,
            "f1_neg": p_class[0], "f1_neu": p_class[1], "f1_pos": p_class[2],
            "train_time": train_time, "pipe": pipe
        })
        
    results = sorted(results, key=lambda x: x["val_f1"], reverse=True)
    top = results[0]
    
    print("\n====================================================================")
    print(f"🏆 TOP FINALIST: {top['name']}")
    print("====================================================================")
    # Stress-test nuanced real-world edge cases across candidates
    print("\n====================================================================")
    print("🧪 STRESS-TESTING CANDIDATES ON REAL-WORLD NUANCED REVIEWS")
    print("====================================================================")
    stress_samples = [
        "Xe đi đường phố thì cực kỳ mượt mà êm ái nhưng mỗi tội tìm trụ sạc pin hơi vất vả.",
        "Tôi thấy tầm giá này thì trang bị tạm ổn, chưa hẳn là xuất sắc nhưng cũng không quá tệ.",
        "Nội thất nhìn thì hào nhoáng nhưng chất lượng gia công ọp ẹp, đi qua gờ kêu lạch cạch khó chịu.",
        "Tầm giá dưới 500 triệu thì không thể đòi hỏi gì hơn, quá hời và tiết kiệm nhiên liệu."
    ]
    for sample in stress_samples:
        print(f"\nReview: \"{sample}\"")
        for r in results:
            pred = r["pipe"].predict([clean_vietnamese_text(sample)])[0]
            print(f"  - {r['name']:<25}: {pred}")

    y_test_pred = top["pipe"].predict(test_df['cleaned_text'])
    test_acc = accuracy_score(test_df['sentiment'], y_test_pred)
    test_f1 = f1_score(test_df['sentiment'], y_test_pred, average='macro', zero_division=0)
    print(f"Holdout Test Accuracy: {test_acc:.4f} | Holdout Test Macro-F1: {test_f1:.4f}\n")
    print(classification_report(test_df['sentiment'], y_test_pred, target_names=classes, digits=4))
    
    # Write Markdown Report
    with open('docs/benchmark_ev_results.md', 'w', encoding='utf-8') as f:
        f.write("# 🚗 EV Sentiment Model Benchmark Report (1,529 Cleaned Reviews)\n\n")
        f.write(f"- **Dataset:** `docs/ev_reviews_vietnam_1529_cleaned.csv` (1,529 unique reviews)\n")
        f.write(f"- **Data Split:** 80% Train ({len(train_df)}), 10% Validation ({len(val_df)}), 10% Test Holdout ({len(test_df)})\n")
        f.write(f"- **MLflow Experiment:** `ev-sentiment-benchmark-standalone`\n\n")
        f.write("## 🏆 Benchmark Leaderboard\n\n")
        f.write("| Rank | Model Candidate | Val Macro-F1 | 5-Fold CV Macro-F1 | Val Acc | F1 (Neg) | F1 (Neu) | F1 (Pos) | Train Latency |\n")
        f.write("| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |\n")
        for i, r in enumerate(results, 1):
            badge = "🥇 **Winner**" if i == 1 else ("🥈 Runner-up" if i == 2 else ("🥉 3rd" if i == 3 else f"#{i}"))
            f.write(f"| {badge} | **{r['name']}** | **{r['val_f1']:.4f}** | {r['cv_mean']:.4f} ± {r['cv_std']:.4f} | {r['val_acc']*100:.2f}% | {r['f1_neg']:.4f} | {r['f1_neu']:.4f} | {r['f1_pos']:.4f} | {r['train_time']:.3f}s |\n")
        f.write(f"\n## 🎯 Finalist Recommendation: {top['name']}\n")
        f.write(f"- **Validation Macro-F1:** `{top['val_f1']:.4f}`\n")
        f.write(f"- **5-Fold CV Macro-F1:** `{top['cv_mean']:.4f} ± {top['cv_std']:.4f}`\n")
        f.write(f"- **Holdout Test Accuracy:** `{test_acc * 100:.2f}%`\n")
        f.write(f"- **Holdout Test Macro-F1:** `{test_f1:.4f}`\n")

if __name__ == '__main__':
    main()

