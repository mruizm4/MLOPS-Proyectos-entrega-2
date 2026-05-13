from locust import HttpUser, task, between

# ============================================================
# LOCUST USER
# ============================================================

class UsuarioDeCarga(HttpUser):

    wait_time = between(0.1, 0.5)

    # --------------------------------------------------------
    # ON START
    # --------------------------------------------------------

    def on_start(self):
        """
        Fetch one valid payload from API.
        """

        self.payload = None

        response = self.client.get(
            "/sample-payload"
        )

        if response.status_code == 200:

            self.payload = (
                response.json()["payload"]
            )

            print(
                "✅ Payload loaded"
            )

        else:

            print(
                "❌ Could not load payload"
            )

    # --------------------------------------------------------
    # PREDICT
    # --------------------------------------------------------

    @task
    def hacer_inferencia(self):

        if self.payload is None:

            return

        response = self.client.post(
            "/predict",
            json=self.payload
        )

        if response.status_code != 200:

            print(
                "❌ Error:"
            )

            print(response.text)