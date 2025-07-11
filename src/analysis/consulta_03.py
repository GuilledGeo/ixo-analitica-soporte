import pandas as pd
import matplotlib.pyplot as plt
import os
import inspect

plt.style.use("ggplot")

def ejecutar():
    nombre_script = os.path.splitext(os.path.basename(inspect.getfile(inspect.currentframe())))[0]
    carpeta_csv = "data/processed"
    archivos = sorted([f for f in os.listdir(carpeta_csv) if f.startswith(nombre_script) and f.endswith(".csv")], reverse=True)
    if not archivos:
        print(f"⚠️ No hay CSV para {nombre_script}")
        return

    ruta_csv = os.path.join(carpeta_csv, archivos[0])
    df = pd.read_csv(ruta_csv)
    print(f"📄 Usando: {ruta_csv}")
    out_dir = os.path.join("outputs", "figures", nombre_script)
    os.makedirs(out_dir, exist_ok=True)

    df["ratio_perdidos"].hist(bins=20, color="#66c2a5")
    plt.title("Distribución del ratio de mensajes perdidos")
    plt.xlabel("Ratio perdidos")
    plt.ylabel("Nº dispositivos")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "01_hist_ratio_perdidos.png"))
    plt.clf()

    df.sort_values("ratio_perdidos", ascending=False).head(15).plot.barh(x="device_id", y="ratio_perdidos", color="#fc8d62")
    plt.title("Top 15 ratio de perdidos más alto")
    plt.xlabel("Ratio perdidos")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "02_top15_ratio_perdidos.png"))
    plt.clf()

    if "mensajes_extra" in df.columns:
        plt.scatter(df["mensajes_extra"], df["ratio_perdidos"], alpha=0.7, color="#8da0cb")
        plt.xlabel("Mensajes extra")
        plt.ylabel("Ratio perdidos")
        plt.title("Extra vs Perdidos")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "03_scatter_extra_vs_perdidos.png"))
        plt.clf()

    if "tipo_dispositivo" in df.columns and df["tipo_dispositivo"].nunique() <= 10:
        df.groupby("tipo_dispositivo")["ratio_perdidos"].mean().sort_values().plot(kind="barh", color="#ffd92f")
        plt.title("Ratio perdidos medio por tipo")
        plt.xlabel("Ratio perdidos")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "04_bar_tipo_dispositivo.png"))
        plt.clf()

    if "customer_name" in df.columns and df["customer_name"].nunique() <= 15:
        df.boxplot(column="ratio_perdidos", by="customer_name", rot=45)
        plt.title("Ratio perdidos por cliente")
        plt.suptitle("")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "05_boxplot_cliente.png"))
        plt.clf()

    if "mensajes_esperados" in df.columns:
        plt.scatter(df["mensajes_esperados"], df["ratio_perdidos"], alpha=0.7, color="#e78ac3")
        plt.xlabel("Mensajes esperados")
        plt.ylabel("Ratio perdidos")
        plt.title("Esperados vs Perdidos")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "06_scatter_esperados_vs_perdidos.png"))
        plt.clf()

    print(f"✅ Figuras guardadas en: {out_dir}")
