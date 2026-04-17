package management

import "testing"

func TestNormalizeOAuthProviderSupportsKiro(t *testing.T) {
	provider, err := NormalizeOAuthProvider("kiro")
	if err != nil {
		t.Fatalf("NormalizeOAuthProvider returned error: %v", err)
	}
	if provider != "kiro" {
		t.Fatalf("NormalizeOAuthProvider = %q, want %q", provider, "kiro")
	}
}

func TestKiroResolveStage1(t *testing.T) {
	redirectURL := "http://127.0.0.1:8317/kiro/init?state=stage1&issuer_url=https%3A%2F%2Fview.awsapps.com%2Fstart&idc_region=us-east-1&login_option=builderid"
	result, err := kiroResolveStage1(redirectURL, "stage1", "", "BuilderId", "", "")
	if err != nil {
		t.Fatalf("kiroResolveStage1 returned error: %v", err)
	}
	if result.IssuerURL != "https://view.awsapps.com/start" {
		t.Fatalf("IssuerURL = %q", result.IssuerURL)
	}
	if result.IDCRegion != "us-east-1" {
		t.Fatalf("IDCRegion = %q", result.IDCRegion)
	}
	if result.LoginOption != "BuilderId" {
		t.Fatalf("LoginOption = %q", result.LoginOption)
	}
}

func TestKiroCredentialFileName(t *testing.T) {
	name := kiroCredentialFileName("User.Name@example.com", "BuilderId")
	if name != "kiro-user-name-at-example-com.json" {
		t.Fatalf("kiroCredentialFileName = %q", name)
	}
}
