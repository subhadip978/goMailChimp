package db

import (
	"fmt"
	"log"

	"github.com/subhadipdas/gomailer/internal/config"
	"github.com/subhadipdas/gomailer/internal/domain"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func ConnectDB(cfg *config.Config) (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC",
		cfg.DBHost, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBPort)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Auto-migrate the database schema
	err = db.AutoMigrate(
		&domain.Template{},
		&domain.TemplateVersion{},
		&domain.Contact{},
		&domain.List{},
		&domain.Campaign{},
		&domain.CampaignJob{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to auto-migrate database: %w", err)
	}

	log.Println("Successfully connected to the PostgreSQL database")
	return db, nil
}
