@echo off
cd /d "C:\Users\h.kemibara\OneDrive - azaniabank.co.tz\Documents\data_analysis\scripts"

:: Add all changes
git add .

:: Commit with timestamp
set CURTIME=%DATE% %TIME%
git commit -m "Auto-commit on %CURTIME%"

:: Push to GitHub
git push origin main

echo ================================
echo ✅ All changes pushed to GitHub!
echo ================================
pause
