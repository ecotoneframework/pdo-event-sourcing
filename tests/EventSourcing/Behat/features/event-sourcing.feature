Feature: activating as aggregate order entity

  Scenario: I order with transaction a product with failure, so the order should never be committed to database
    Given I active messaging for namespace "Test\Ecotone\EventSourcing\Fixture\Ticket"
