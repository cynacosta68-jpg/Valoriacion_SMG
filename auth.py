"""
auth.py — Módulo de autenticación para apps hijas
═══════════════════════════════════════════════════

INSTRUCCIONES:
1. Copiá este archivo en el repo de cada app hija
2. Configurá los secrets en Streamlit Cloud (Settings → Secrets)
3. Agregá estas líneas al inicio de tu app:

    from auth import login_required
    login_required()

    # --- Tu código normal a partir de acá ---
    st.title("Mi App")
"""

import streamlit as st
import hmac

# URL del Hub
HUB_URL = "https://repositorioapps-vyorfsc6sacrtffgxi8xgs.streamlit.app/"


def _verificar_credenciales():
    """Callback del botón Ingresar."""
    username = st.session_state.get("_auth_user", "")
    password = st.session_state.get("_auth_pass", "")
    users = st.secrets.get("usuarios", {})

    if username in users and hmac.compare_digest(password, users[username]):
        st.session_state["_autenticado"] = True
        st.session_state["_usuario"] = username
        del st.session_state["_auth_pass"]
    else:
        st.session_state["_autenticado"] = False
        st.session_state["_auth_error"] = True


def login_required():
    """
    Bloquea la app hasta que el usuario se autentique.
    Llamar al inicio de cada app hija.
    """
    # Ya autenticado → mostrar barra superior y seguir
    if st.session_state.get("_autenticado", False):
        _mostrar_barra_superior()
        return

    # CSS para el login
    st.markdown("""
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .login-box {
            max-width: 420px;
            margin: 6vh auto 1vh;
            padding: 2rem 2rem 0.5rem;
            border: 1px solid rgba(128,128,128,0.2);
            border-radius: 16px;
            background: rgba(128,128,128,0.03);
            text-align: center;
        }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="login-box">
        <h2>🔐 Acceso requerido</h2>
        <p style="color:gray; font-size:0.9rem;">Ingresá tus credenciales para usar esta app</p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        st.text_input("Usuario", key="_auth_user")
        st.text_input("Contraseña", type="password", key="_auth_pass")
        st.button("Ingresar", on_click=_verificar_credenciales, use_container_width=True, type="primary")

        if st.session_state.get("_auth_error", False):
            st.error("❌ Usuario o contraseña incorrectos")

        # Link para volver al hub
        st.markdown(
            f'<p style="text-align:center; margin-top:1rem;">'
            f'<a href="{HUB_URL}" target="_self">← Volver al Hub</a></p>',
            unsafe_allow_html=True,
        )

    st.stop()


def _mostrar_barra_superior():
    """Muestra barra con usuario logueado + botones."""
    col1, col2, col3 = st.columns([4, 1, 1])
    with col1:
        st.caption(f"👤 {st.session_state.get('_usuario', '')}")
    with col2:
        st.link_button("🏠 Hub", HUB_URL, use_container_width=True)
    with col3:
        if st.button("🚪 Salir", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


def get_usuario():
    """Retorna el nombre del usuario logueado (útil para logging/auditoría)."""
    return st.session_state.get("_usuario", None)
