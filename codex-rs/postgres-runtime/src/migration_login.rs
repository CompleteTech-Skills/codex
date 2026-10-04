//! Separate authenticated migration login; no runtime-role elevation path.
use crate::NamedNamespace;
use sqlx::AssertSqlSafe;
use sqlx::PgConnection;
#[derive(Clone)]
pub(crate) struct MigrationLogin {
    login: String,
    owner: String,
    quoted_owner: String,
    causes: std::sync::Arc<crate::migration_causes::MigrationCauses>,
}
impl MigrationLogin {
    pub(crate) fn new(namespace: Option<&NamedNamespace>) -> Self {
        match namespace {
            None => Self {
                login: "codex_migrator".to_string(),
                owner: "codex_owner".to_string(),
                quoted_owner: "\"codex_owner\"".to_string(),
                causes: crate::migration_causes::MigrationCauses::new(),
            },
            Some(namespace) => Self {
                login: namespace.migrator_login().to_string(),
                owner: namespace.owner.clone(),
                quoted_owner: namespace.quoted_owner(),
                causes: crate::migration_causes::MigrationCauses::new(),
            },
        }
    }
    pub(crate) fn retained_cause_count(&self) -> Result<usize, sqlx::Error> {
        self.causes.retained_count()
    }
    pub(crate) fn reserve_driver(
        &self,
    ) -> Result<crate::migration_causes::Reservation, sqlx::Error> {
        self.causes.reserve()
    }
    pub(crate) fn accepts_login(&self, username: &str) -> bool {
        username == self.login
    }
    /// Run on EVERY new physical connection, including replacement/reconnect.
    pub(crate) async fn initialize(
        &self,
        connection: &mut PgConnection,
    ) -> Result<(), sqlx::Error> {
        let reservation = self.causes.reserve()?;
        let result = self.initialize_original(connection).await;
        match result {
            Ok(()) => {
                reservation.success();
                Ok(())
            }
            Err(original) => Err(reservation.failure(original)),
        }
    }
    async fn initialize_original(&self, connection: &mut PgConnection) -> Result<(), sqlx::Error> {
        let admitted: bool = sqlx::query_scalar(
            "SELECT session_user::text=$1 AND EXISTS(SELECT 1 FROM pg_catalog.pg_roles \
             WHERE rolname=session_user AND rolcanlogin AND NOT rolinherit AND NOT rolsuper \
             AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls) AND EXISTS(SELECT 1 FROM pg_catalog.pg_roles \
             WHERE rolname=$2 AND NOT rolcanlogin AND NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls) \
             AND pg_catalog.pg_has_role(session_user,$2::pg_catalog.name,'SET')")
            .bind(&self.login).bind(&self.owner).fetch_one(&mut *connection).await?;
        if !admitted {
            return Err(sqlx::Error::Protocol(
                "authenticated migration login refused".to_string(),
            ));
        }
        // Derived only from default literal or validated NamedNamespace's centralized quoting.
        sqlx::query(AssertSqlSafe(format!("SET ROLE {}", self.quoted_owner)))
            .execute(&mut *connection)
            .await?;
        let selected: bool =
            sqlx::query_scalar("SELECT session_user::text=$1 AND current_user::text=$2")
                .bind(&self.login)
                .bind(&self.owner)
                .fetch_one(connection)
                .await?;
        if !selected {
            return Err(sqlx::Error::Protocol(
                "authenticated migration owner selection refused".to_string(),
            ));
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runtime_and_foreign_login_cannot_request_migration_owner_selection()
    -> Result<(), crate::InvalidNamespace> {
        let default = MigrationLogin::new(None);
        assert!(default.accepts_login("codex_migrator"));
        assert!(!default.accepts_login("codex_runtime"));
        assert!(!default.accepts_login("codex_other_migrator"));
        let namespace = NamedNamespace::new("codex_storage_isolation")?;
        let named = MigrationLogin::new(Some(&namespace));
        assert!(named.accepts_login(namespace.migrator_login()));
        assert!(!named.accepts_login(namespace.runtime_login()));
        assert!(!named.accepts_login("codex_migrator"));
        assert_eq!(named.quoted_owner, namespace.quoted_owner());
        Ok(())
    }
}
