package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const defaultRegion = "us-east-1"

type legacyKiroCredential struct {
	ID                int    `json:"id"`
	AccessToken       string `json:"accessToken"`
	RefreshToken      string `json:"refreshToken"`
	ProfileARN        string `json:"profileArn"`
	ExpiresAt         string `json:"expiresAt"`
	AuthMethod        string `json:"authMethod"`
	Provider          string `json:"provider"`
	ClientID          string `json:"clientId"`
	ClientSecret      string `json:"clientSecret"`
	AuthRegion        string `json:"authRegion"`
	APIRegion         string `json:"apiRegion"`
	MachineID         string `json:"machineId"`
	Email             string `json:"email"`
	SubscriptionTitle string `json:"subscriptionTitle"`
	Disabled          bool   `json:"disabled"`
}

type kiroAuthFile struct {
	Type              string            `json:"type"`
	Provider          string            `json:"provider"`
	AuthMethod        string            `json:"auth_method"`
	ClientID          string            `json:"client_id"`
	ClientSecret      string            `json:"client_secret"`
	RefreshToken      string            `json:"refresh_token"`
	AccessToken       string            `json:"access_token"`
	ExpiresAt         string            `json:"expires_at"`
	ProfileARN        string            `json:"profile_arn"`
	Region            string            `json:"region"`
	APIRegion         string            `json:"api_region"`
	MachineID         string            `json:"machine_id"`
	Email             string            `json:"email"`
	SubscriptionTitle string            `json:"subscription_title"`
	ProxyURL          string            `json:"proxy_url"`
	ProxyUsername     string            `json:"proxy_username"`
	ProxyPassword     string            `json:"proxy_password"`
	Prefix            string            `json:"prefix"`
	Headers           map[string]string `json:"headers"`
	ExcludedModels    []string          `json:"excluded_models"`
	Priority          int               `json:"priority"`
	Note              string            `json:"note"`
	Disabled          bool              `json:"disabled"`
}

func main() {
	inputPath := flag.String("input", "auths/credentials.json", "Path to the Kiro credentials array JSON file")
	outputDir := flag.String("output-dir", "auths", "Directory where converted auth files will be written")
	namePrefix := flag.String("name-prefix", "kiro-builderid-imported", "Output filename prefix without numeric suffix")
	overwrite := flag.Bool("overwrite", false, "Overwrite existing files with the same name")
	pretty := flag.Bool("pretty", true, "Write formatted JSON")
	flag.Parse()

	inputData, err := os.ReadFile(*inputPath)
	if err != nil {
		exitf("read input %s: %v", *inputPath, err)
	}

	var entries []legacyKiroCredential
	if err = json.Unmarshal(inputData, &entries); err != nil {
		exitf("decode input %s: %v", *inputPath, err)
	}
	if len(entries) == 0 {
		exitf("input %s contains no credentials", *inputPath)
	}

	if err = os.MkdirAll(*outputDir, 0o700); err != nil {
		exitf("create output dir %s: %v", *outputDir, err)
	}

	written := 0
	skipped := 0
	for index, entry := range entries {
		fileName := buildFileName(*namePrefix, entry, index)
		targetPath := filepath.Join(*outputDir, fileName)
		if !*overwrite {
			if _, errStat := os.Stat(targetPath); errStat == nil {
				fmt.Printf("skip existing: %s\n", targetPath)
				skipped++
				continue
			} else if !os.IsNotExist(errStat) {
				exitf("stat output %s: %v", targetPath, errStat)
			}
		}

		payload := convertCredential(entry)
		raw, errMarshal := marshalOutput(payload, *pretty)
		if errMarshal != nil {
			exitf("marshal %s: %v", targetPath, errMarshal)
		}
		if errWrite := os.WriteFile(targetPath, raw, 0o600); errWrite != nil {
			exitf("write %s: %v", targetPath, errWrite)
		}
		fmt.Printf("wrote: %s\n", targetPath)
		written++
	}

	fmt.Printf("done: wrote=%d skipped=%d total=%d\n", written, skipped, len(entries))
}

func convertCredential(src legacyKiroCredential) kiroAuthFile {
	region := firstNonEmpty(src.AuthRegion, src.APIRegion, regionFromProfileARN(src.ProfileARN), defaultRegion)
	apiRegion := firstNonEmpty(src.APIRegion, src.AuthRegion, region)
	return kiroAuthFile{
		Type:              "kiro",
		Provider:          firstNonEmpty(strings.TrimSpace(src.Provider), "BuilderId"),
		AuthMethod:        firstNonEmpty(strings.TrimSpace(src.AuthMethod), "idc"),
		ClientID:          strings.TrimSpace(src.ClientID),
		ClientSecret:      strings.TrimSpace(src.ClientSecret),
		RefreshToken:      strings.TrimSpace(src.RefreshToken),
		AccessToken:       strings.TrimSpace(src.AccessToken),
		ExpiresAt:         strings.TrimSpace(src.ExpiresAt),
		ProfileARN:        strings.TrimSpace(src.ProfileARN),
		Region:            region,
		APIRegion:         apiRegion,
		MachineID:         strings.TrimSpace(src.MachineID),
		Email:             strings.TrimSpace(src.Email),
		SubscriptionTitle: strings.TrimSpace(src.SubscriptionTitle),
		ProxyURL:          "",
		ProxyUsername:     "",
		ProxyPassword:     "",
		Prefix:            "",
		Headers:           map[string]string{},
		ExcludedModels:    []string{},
		Priority:          0,
		Note:              "",
		Disabled:          src.Disabled,
	}
}

func marshalOutput(payload kiroAuthFile, pretty bool) ([]byte, error) {
	if pretty {
		raw, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return nil, err
		}
		return append(raw, '\n'), nil
	}
	return json.Marshal(payload)
}

func buildFileName(prefix string, entry legacyKiroCredential, index int) string {
	safePrefix := sanitizeFilePart(prefix)
	if safePrefix == "" {
		safePrefix = "kiro-imported"
	}
	identifier := entry.ID
	if identifier <= 0 {
		identifier = index + 1
	}
	return safePrefix + "-" + zeroPad(identifier, 4) + ".json"
}

func sanitizeFilePart(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var builder strings.Builder
	lastDash := false
	for _, r := range value {
		isAllowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAllowed {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if lastDash {
			continue
		}
		builder.WriteByte('-')
		lastDash = true
	}
	return strings.Trim(builder.String(), "-")
}

func zeroPad(value, width int) string {
	text := strconv.Itoa(value)
	if len(text) >= width {
		return text
	}
	return strings.Repeat("0", width-len(text)) + text
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func regionFromProfileARN(profileARN string) string {
	parts := strings.Split(strings.TrimSpace(profileARN), ":")
	if len(parts) >= 4 {
		return strings.TrimSpace(parts[3])
	}
	return ""
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
