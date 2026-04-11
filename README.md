# The Ultimate Command Center for Your Wealth.

> **Absolute Privacy. Stunning Clarity. Unparalleled Control.**

Welcome to the **Portfolio Tracker** — a revolutionary, local-first fortress for your financial data. Designed with breathtaking aesthetics and engineered for total dominance over your investments, this is not just a tracker; it is your definitive advantage in the markets. 

## Command Your Wealth, Without Compromise

We believe that true power lies in absolute control. Most portfolio trackers force you to surrender your most sensitive financial data to the cloud. Not here. 

By running **entirely on your local machine**, your data remains completely concealed from the world. Your intentions, your holdings, and your strategies stay strictly yours. Your data yields to no one but you.

## Effortless Mastery Through Action

Don't just observe the market—dominate it. With a meticulously crafted, glassmorphic dark-mode dashboard, the complexity of your investments is distilled into pure, actionable insight. Win through decisive action fueled by pristine metrics, not through speculation.

- **Seamless Data Ingestion:** Effortlessly import your broker's CSVs. Our intelligent parser adapts powerfully to different formats across Indian brokers, eliminating friction instantly.
- **Real-Time Superiority:** Live pricing fueled by Yahoo Finance ensures your insights are relentlessly up-to-date. Master the art of timing.
- **Architectural Supremacy:** Powered by Python and SQLite, this engine is robust, unyielding, and ready to ingest massive portfolios without a stutter. 

## Features that Empower

- **The Omniscient Dashboard:** Visualize total value, P&L, intelligent returns, and an exclusive portfolio health score in one gorgeous, unified view. 
- **Surgical Sector Allocation:** Interactive, fluid charts reveal your exact market exposure. Never leave your portfolio vulnerable or unbalanced.
- **Absolute Foresight:** Track historical trends, log meticulous transactions, and manage multiple portfolios with zero friction.
- **Watchlist of the Ambitious:** Maintain a strict list of targets. Track live movements and strike only when the valuation is absolutely perfect. 

## System Architecture & Required Structure

Because this fortress runs on Flask, you must maintain the strict folder architecture shown below. If you upload the files to GitHub without preserving these exact folders, the dashboard will lose its styling and fail to load properly.

```text
Portfolio-Tracker/
├── app.py                   # The core routing engine
├── data_processor.py        # Intelligent CSV parser
├── price_fetcher.py         # Live Yahoo Finance integration
├── database.py              # SQLite architect
├── xirr_calculator.py       # Advanced returns calculator
├── templates/               
│   └── index.html           # The Dashboard layout (REQUIRED FOLDER)
├── static/                  
│   ├── css/style.css        # The UI aesthetics (REQUIRED FOLDER)
│   └── js/app.js            # Frontend logic
├── README.md
└── requirements.txt
```

> [!WARNING]
> **Important Note for GitHub Uploads:**  
> Because GitHub's web interface often blocks dragging and dropping folders directly from your computer, you must be careful when uploading. To preserve the structure:
> 1. **Best Method:** Upload via the Terminal using Git (`git push`).
> 2. **Browser Method:** If using the web browser, open your repo, click "Add file" -> "Upload files", and **drag the `templates` and `static` folders directly from Mac Finder** into the browser box. Do not use the clickable "choose your files" menu, as it will strip the folders and break the application.

## Deploying Your Fortress

Setting up your command center is phenomenal and effortless.

**Prerequisites:** 
- **Python 3.9+** is required.
- Ensure the project structure maintains the `templates/` and `static/` directories exactly as they are—they power the dashboard's aesthetics and functionality.

```bash
# 1. Establish the foundation
git clone https://github.com/Siddh-Parikh/Portfolio-Tracker.git
cd Portfolio-Tracker

# 2. Forge the environment
pip install -r requirements.txt

# 3. Ignite the engine
python app.py
```
*Open `http://localhost:5000` and step into absolute command.*

## Ingesting Your Data

The system is designed to consume standard broker exports with zero manual manipulation. Play to your strengths—let the machine handle the data alignment.

1. **Holdings CSV (Required):** The bedrock of your portfolio, directly from your broker. 
2. **Gain/Loss CSV (Optional):** Historical cost-basis for surgical P&L accuracy.
3. **Symbol Map (Optional):** A bespoke file to forcefully map obscure ISINs to Yahoo Finance tickers when you need total control over the data pipeline.

## The Open Source Advantage

This tool is forged for the ambitious. The code is entirely open source, inviting you to inspect, adapt, and refine the engine to suit your unique vision. 

Fork it, build upon it, and crush the market—but always maintain absolute control over your wealth.
