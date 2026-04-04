use crate::service::AwsService;
use std::collections::HashMap;
use std::sync::Arc;

/// Registry of AWS services available in this FakeCloud instance.
#[derive(Default)]
pub struct ServiceRegistry {
    services: HashMap<String, Arc<dyn AwsService>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, service: Arc<dyn AwsService>) {
        self.services
            .insert(service.service_name().to_string(), service);
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn AwsService>> {
        self.services.get(name)
    }

    pub fn service_names(&self) -> Vec<&str> {
        self.services.keys().map(|s| s.as_str()).collect()
    }
}
