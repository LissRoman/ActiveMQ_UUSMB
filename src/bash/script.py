import subprocess

class script():
    """Simula la tarea de procesamiento de Watson."""
    def __init__(self, project_id):
        self.project_id = project_id
        self.sh_file = "/home/liss/script-liss.sh"  # Ruta del archivo .sh

    def run_script(self):
        try:
            print(f"Ejecutando script para el project_id: {self.project_id}")
            # Ejecutar el contenido del archivo .sh
            process = subprocess.run(
                ["bash", self.sh_file],
                check=True,
                text=True,
            )

            print(process.stdout)  # Imprimir la salida estándar

        except FileNotFoundError:
            print(f"El archivo {self.sh_file} no existe.")

        except subprocess.CalledProcessError as e:
            print("El script terminó con error")
            print(f"Error: {e.stderr}")