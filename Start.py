import subprocess
import sys
import os


venv_path = r"C:\Users\Rick\PycharmProjects\RPFFT\venv"

if os.path.isdir(venv_path):
    activate_script = os.path.join(venv_path, "Scripts", "activate")
    if sys.platform != "win32":
        activate_script = "source " + os.path.join(venv_path, "bin", "activate")


    subprocess.Popen(activate_script, shell=True)


    tasks_script_path = r"C:\Users\Rick\PycharmProjects\RPFFT\TasksMessages.py"
    tasks_process = subprocess.Popen([sys.executable, tasks_script_path])


    tasks_process.wait()

# Запуск файла ResultsMessages.py в отдельном процессе
results_process = subprocess.Popen([sys.executable, r"C:\Users\Rick\PycharmProjects\RPFFT\ResultsMessages.py"])


tasks_process.wait()
results_process.wait()

print("Программа успешно завершена.")