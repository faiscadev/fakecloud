use crate::state::Route;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RouteMatch {
    pub route: Route,
    pub path_parameters: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct ParsedRoute {
    method: String,
    segments: Vec<RouteSegment>,
    priority: i32,
}

#[derive(Debug, Clone)]
enum RouteSegment {
    Exact(String),
    Parameter(String),
    Greedy(String),
}

impl ParsedRoute {
    fn from_route_key(route_key: &str) -> Option<Self> {
        let parts: Vec<&str> = route_key.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return None;
        }

        let method = parts[0].to_string();
        let path = parts[1];

        let segments = Self::parse_path(path);
        let priority = Self::calculate_priority(&method, &segments);

        Some(Self {
            method,
            segments,
            priority,
        })
    }

    fn parse_path(path: &str) -> Vec<RouteSegment> {
        path.trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|seg| {
                if seg.starts_with('{') && seg.ends_with('}') {
                    let param_name = seg.trim_start_matches('{').trim_end_matches('}');
                    if param_name.ends_with('+') {
                        RouteSegment::Greedy(param_name.trim_end_matches('+').to_string())
                    } else {
                        RouteSegment::Parameter(param_name.to_string())
                    }
                } else {
                    RouteSegment::Exact(seg.to_string())
                }
            })
            .collect()
    }

    fn calculate_priority(method: &str, segments: &[RouteSegment]) -> i32 {
        let mut priority = 0;
        for seg in segments {
            priority += match seg {
                RouteSegment::Exact(_) => 100,
                RouteSegment::Parameter(_) => 50,
                RouteSegment::Greedy(_) => 10,
            };
        }
        // ANY routes should have lower priority than exact-method routes
        if method == "ANY" {
            priority -= 1;
        }
        priority
    }

    fn matches(&self, method: &str, path_segments: &[String]) -> Option<HashMap<String, String>> {
        // Check method match (ANY matches all methods)
        if self.method != "ANY" && self.method != method {
            return None;
        }

        let mut params = HashMap::new();
        let mut route_idx = 0;
        let mut path_idx = 0;

        while route_idx < self.segments.len() {
            match &self.segments[route_idx] {
                RouteSegment::Exact(expected) => {
                    if path_idx >= path_segments.len() || path_segments[path_idx] != *expected {
                        return None;
                    }
                    path_idx += 1;
                }
                RouteSegment::Parameter(name) => {
                    if path_idx >= path_segments.len() {
                        return None;
                    }
                    params.insert(name.clone(), path_segments[path_idx].clone());
                    path_idx += 1;
                }
                RouteSegment::Greedy(name) => {
                    // Greedy parameter consumes all remaining segments
                    if path_idx >= path_segments.len() {
                        return None;
                    }
                    let remaining = path_segments[path_idx..].join("/");
                    params.insert(name.clone(), remaining);
                    path_idx = path_segments.len();
                }
            }
            route_idx += 1;
        }

        // All segments must be consumed
        if path_idx != path_segments.len() {
            return None;
        }

        Some(params)
    }
}

pub struct Router {
    routes: Vec<(Route, ParsedRoute)>,
}

impl Router {
    pub fn new(routes: Vec<Route>) -> Self {
        let mut parsed_routes: Vec<(Route, ParsedRoute)> = routes
            .into_iter()
            .filter_map(|route| {
                ParsedRoute::from_route_key(&route.route_key).map(|parsed| (route, parsed))
            })
            .collect();

        // Sort by priority (highest first)
        parsed_routes.sort_by(|a, b| b.1.priority.cmp(&a.1.priority));

        Self {
            routes: parsed_routes,
        }
    }

    pub fn match_route(&self, method: &str, path: &str) -> Option<RouteMatch> {
        let path_segments: Vec<String> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        for (route, parsed) in &self.routes {
            if let Some(params) = parsed.matches(method, &path_segments) {
                return Some(RouteMatch {
                    route: route.clone(),
                    path_parameters: params,
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_path() {
        let routes = vec![Route {
            route_id: "r1".to_string(),
            route_key: "GET /pets".to_string(),
            target: None,
            authorization_type: None,
            authorizer_id: None,
        }];

        let router = Router::new(routes);
        let result = router.match_route("GET", "/pets");
        assert!(result.is_some());
        let route_match = result.unwrap();
        assert_eq!(route_match.route.route_id, "r1");
        assert!(route_match.path_parameters.is_empty());
    }

    #[test]
    fn test_path_parameter() {
        let routes = vec![Route {
            route_id: "r1".to_string(),
            route_key: "GET /pets/{id}".to_string(),
            target: None,
            authorization_type: None,
            authorizer_id: None,
        }];

        let router = Router::new(routes);
        let result = router.match_route("GET", "/pets/123");
        assert!(result.is_some());
        let route_match = result.unwrap();
        assert_eq!(route_match.route.route_id, "r1");
        assert_eq!(
            route_match.path_parameters.get("id"),
            Some(&"123".to_string())
        );
    }

    #[test]
    fn test_greedy_parameter() {
        let routes = vec![Route {
            route_id: "r1".to_string(),
            route_key: "GET /api/{proxy+}".to_string(),
            target: None,
            authorization_type: None,
            authorizer_id: None,
        }];

        let router = Router::new(routes);
        let result = router.match_route("GET", "/api/v1/users/123");
        assert!(result.is_some());
        let route_match = result.unwrap();
        assert_eq!(route_match.route.route_id, "r1");
        assert_eq!(
            route_match.path_parameters.get("proxy"),
            Some(&"v1/users/123".to_string())
        );
    }

    #[test]
    fn test_priority_exact_over_parameter() {
        let routes = vec![
            Route {
                route_id: "r1".to_string(),
                route_key: "GET /pets/{id}".to_string(),
                target: None,
                authorization_type: None,
                authorizer_id: None,
            },
            Route {
                route_id: "r2".to_string(),
                route_key: "GET /pets/special".to_string(),
                target: None,
                authorization_type: None,
                authorizer_id: None,
            },
        ];

        let router = Router::new(routes);
        let result = router.match_route("GET", "/pets/special");
        assert!(result.is_some());
        let route_match = result.unwrap();
        assert_eq!(route_match.route.route_id, "r2"); // Exact match wins
    }

    #[test]
    fn test_priority_parameter_over_greedy() {
        let routes = vec![
            Route {
                route_id: "r1".to_string(),
                route_key: "GET /api/{proxy+}".to_string(),
                target: None,
                authorization_type: None,
                authorizer_id: None,
            },
            Route {
                route_id: "r2".to_string(),
                route_key: "GET /api/{version}/users".to_string(),
                target: None,
                authorization_type: None,
                authorizer_id: None,
            },
        ];

        let router = Router::new(routes);
        let result = router.match_route("GET", "/api/v1/users");
        assert!(result.is_some());
        let route_match = result.unwrap();
        assert_eq!(route_match.route.route_id, "r2"); // Parameter match wins over greedy
    }

    #[test]
    fn test_any_method() {
        let routes = vec![Route {
            route_id: "r1".to_string(),
            route_key: "ANY /pets".to_string(),
            target: None,
            authorization_type: None,
            authorizer_id: None,
        }];

        let router = Router::new(routes);
        assert!(router.match_route("GET", "/pets").is_some());
        assert!(router.match_route("POST", "/pets").is_some());
        assert!(router.match_route("DELETE", "/pets").is_some());
    }

    #[test]
    fn test_exact_method_over_any() {
        let routes = vec![
            Route {
                route_id: "any".to_string(),
                route_key: "ANY /pets".to_string(),
                target: None,
                authorization_type: None,
                authorizer_id: None,
            },
            Route {
                route_id: "get".to_string(),
                route_key: "GET /pets".to_string(),
                target: None,
                authorization_type: None,
                authorizer_id: None,
            },
        ];

        let router = Router::new(routes);
        let result = router.match_route("GET", "/pets");
        assert!(result.is_some());
        assert_eq!(result.unwrap().route.route_id, "get"); // Exact method wins over ANY
    }

    #[test]
    fn test_no_match() {
        let routes = vec![Route {
            route_id: "r1".to_string(),
            route_key: "GET /pets".to_string(),
            target: None,
            authorization_type: None,
            authorizer_id: None,
        }];

        let router = Router::new(routes);
        assert!(router.match_route("GET", "/users").is_none());
        assert!(router.match_route("POST", "/pets").is_none());
    }

    #[test]
    fn test_multiple_parameters() {
        let routes = vec![Route {
            route_id: "r1".to_string(),
            route_key: "GET /users/{userId}/posts/{postId}".to_string(),
            target: None,
            authorization_type: None,
            authorizer_id: None,
        }];

        let router = Router::new(routes);
        let result = router.match_route("GET", "/users/123/posts/456");
        assert!(result.is_some());
        let route_match = result.unwrap();
        assert_eq!(
            route_match.path_parameters.get("userId"),
            Some(&"123".to_string())
        );
        assert_eq!(
            route_match.path_parameters.get("postId"),
            Some(&"456".to_string())
        );
    }
}
