Feature: activating as aggregate order entity

  Scenario: I verify building projection from event sourced aggregate
    Given I active messaging for namespace "Test\Ecotone\EventSourcing\Fixture\Ticket"
    When I register "alert" ticket 123 with assignation to "Johny"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 123        | alert          |
    When I close ticket with id 123
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |

  Scenario: Operations on the projection
    Given I active messaging for namespace "Test\Ecotone\EventSourcing\Fixture\Ticket"
    When I register "alert" ticket 1234 with assignation to "Marcus"
    And I stop the projection for in progress tickets
    And I register "alert" ticket 12345 with assignation to "Andrew"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 1234       | alert          |
    When I reset the projection for in progress tickets
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 1234       | alert          |
      | 12345      | alert          |
    And I delete projection for all in progress tickets
    Then there should be no in progress ticket list